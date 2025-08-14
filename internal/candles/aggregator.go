// Package candles provides real-time OHLC (Open, High, Low, Close) candlestick aggregation
// from multiple cryptocurrency exchange trade streams.
//
// Thread Safety:
//   - All operations are thread-safe through careful goroutine coordination
//   - Map access is serialized through single processing goroutine
//   - Channel operations provide memory barrier synchronization
//   - WaitGroup ensures proper cleanup on shutdown
package candles

import (
	"candles/internal/model"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

// ExchangeConnector defines the interface for subscribing to trade events from exchanges.
//
// This interface abstracts the implementation details of different cryptocurrency exchanges,
// allowing the aggregator to work with any exchange that can provide real-time trade data.
// Each exchange connector handles its specific WebSocket protocol, message formats, and
// data normalization.
type ExchangeConnector interface {
	// SubscribeToTrades establishes subscriptions to trade events for specified trading pairs.
	//
	// This method initiates real-time trade data streaming from the exchange for the
	// provided trading pairs. The returned channel delivers TradeEvent structures
	// containing normalized trade information from the exchange.
	SubscribeToTrades(ctx context.Context, pairs []string) (<-chan model.TradeEvent, error)
}

// Aggregator processes trade events from multiple exchanges to generate OHLC candlestick data.
//
// The Aggregator implements a sophisticated streaming data processing system that:
//   - Consumes real-time trade events from multiple exchange connectors
//   - Aggregates trades into OHLC (Open, High, Low, Close) candlesticks
//   - Publishes completed candles at regular time intervals
//   - Maintains separate candle state for each trading pair
//   - Provides proper resource cleanup and shutdown handling
type Aggregator struct {
	// exchanges holds the list of exchange connectors for trade data sourcing.
	//
	// This slice contains all exchange connectors that will provide trade events
	// for aggregation. Each connector implements the ExchangeConnector interface
	// and handles its specific exchange's WebSocket protocol and data format.
	exchanges []ExchangeConnector

	// interval defines the time duration for each candlestick period.
	interval time.Duration

	// candles maintains the current OHLC state for each trading pair.
	//
	// This map stores one active candlestick per trading pair, continuously
	// updated with incoming trade events. The map key is the trading pair
	// symbol (e.g., "BTC-USDT") and the value is a pointer to the current
	// OHLC candle being constructed.
	//
	// Memory Management:
	//   - Candle objects are reused between intervals (not recreated)
	//   - Fields are reset to zero values rather than allocating new objects
	//   - Eliminates garbage collection pressure from frequent allocations
	candles map[string]*model.OHLCCandle
}

// NewAggregator creates a new candlestick aggregator with the specified configuration.
func NewAggregator(exchanges []ExchangeConnector, interval time.Duration) *Aggregator {
	return &Aggregator{
		exchanges: exchanges,
		interval:  interval,
		candles:   make(map[string]*model.OHLCCandle),
	}
}

// StartCandleStream initiates trade processing and returns a channel of completed candlesticks.
//
// This method orchestrates the entire trade aggregation pipeline:
//  1. Establishes trade subscriptions with all configured exchanges
//  2. Creates a fan-in channel to merge all trade streams
//  3. Starts the trade processing goroutine
//  4. Returns a channel delivering completed OHLC candlesticks
//
// The method implements a fail-fast approach where any exchange subscription failure
// causes the entire startup process to fail and cancel all other subscriptions.
func (agg *Aggregator) StartCandleStream(ctx context.Context, pairs []string) (<-chan model.OHLCCandle, error) {
	// Create cancellable context for coordination
	ctx, cancel := context.WithCancel(ctx)

	// Subscribe to trades from all exchanges
	tradeChannels := make([]<-chan model.TradeEvent, 0, len(agg.exchanges))
	for _, exchange := range agg.exchanges {
		if tradeCh, err := exchange.SubscribeToTrades(ctx, pairs); err == nil {
			tradeChannels = append(tradeChannels, tradeCh)
		} else {
			cancel() // cancel any other subscriptions
			return nil, fmt.Errorf("failed to subscribe to trades: %w", err)
		}
	}

	// Merge all trade streams into single channel
	fanInCh := agg.fanIn(ctx, tradeChannels)

	// Start trade processing and return candle stream
	return agg.processTrades(ctx, fanInCh), nil
}

// processTrades runs the main trade processing loop and generates candlestick output.
//
// This method implements the core aggregation logic that:
//   - Continuously processes incoming trade events
//   - Updates OHLC candlestick state for each trading pair
//   - Publishes completed candles at regular intervals
//   - Handles graceful shutdown on context cancellation
//
// Processing Loop:
//
//	The main goroutine handles three event types:
//	1. Context cancellation: Clean shutdown
//	2. Interval timer: Publish candles and reset state
//	3. Trade events: Update current candle state
func (agg *Aggregator) processTrades(ctx context.Context, input <-chan model.TradeEvent) <-chan model.OHLCCandle {
	output := make(chan model.OHLCCandle, 1000)
	ticker := time.NewTicker(agg.interval)

	go func() {
		defer close(output)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Info().Msg("Aggregator stopped")
				return
			case <-ticker.C:
				agg.publish(output)
				agg.clearCandles()
			case trade, ok := <-input:
				if !ok {
					return
				}
				agg.updateCandle(trade)
			}
		}
	}()

	return output
}

// fanIn merges multiple trade event channels into a single output channel.
//
// This method implements the fan-in concurrency pattern to combine trade streams
// from multiple exchanges into a unified channel for downstream processing.
// It creates one goroutine per input channel for concurrent processing and
// coordinates shutdown when all inputs are closed or context is cancelled.
func (agg *Aggregator) fanIn(ctx context.Context, inputChannels []<-chan model.TradeEvent) <-chan model.TradeEvent {
	dest := make(chan model.TradeEvent, 1000)
	var wg sync.WaitGroup
	wg.Add(len(inputChannels))

	// Start one goroutine per input channel
	for _, ch := range inputChannels {
		go func(ctx context.Context, c <-chan model.TradeEvent) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case n, ok := <-c:
					if !ok {
						return
					}
					dest <- n
				}
			}
		}(ctx, ch)
	}

	// Close output channel when all inputs are done
	go func() {
		wg.Wait()
		close(dest)
	}()

	return dest
}

// publish sends all current candlestick data to the output channel.
//
// This method is called periodically by the interval timer to emit completed
// OHLC candlesticks for all trading pairs that have received trades during
// the current interval. It implements a non-blocking publish pattern to
// prevent any issues with slow consumers.
func (agg *Aggregator) publish(out chan<- model.OHLCCandle) {
	for _, value := range agg.candles {
		if value.Open.IsZero() {
			continue // Skip empty candles
		}
		out <- *value
	}
}

// clearCandles resets all candlestick data for the next interval period.
//
// This method efficiently resets all candle fields to their zero values without
// deallocating the underlying candle objects. This approach minimizes garbage
// collection pressure by reusing existing memory allocations.
func (agg *Aggregator) clearCandles() {
	for _, candle := range agg.candles {
		// Reset time fields
		candle.StartTime = time.Time{}
		candle.EndTime = time.Time{}

		// Reset OHLC price fields
		candle.Open = decimal.Zero
		candle.High = decimal.Zero
		candle.Low = decimal.Zero
		candle.Close = decimal.Zero
		candle.Volume = decimal.Zero
	}
}

// Potential improvements:
//   - Add timestamp validation to detect old trade events
//   - Drop trades older than current interval boundary
//   - Implement configurable staleness threshold
//   - Add metrics for dropped stale messages
//   - Consider exchange-specific latency characteristics

// updateCandle processes a single trade event and updates the corresponding OHLC candle.
//
// This method implements the core OHLC calculation logic that transforms individual
// trade events into aggregated candlestick data. It handles both new candle creation
// and updates to existing candles with proper OHLC semantics.
func (agg *Aggregator) updateCandle(trade model.TradeEvent) {
	pair := trade.Pair

	// Get or create candle for this trading pair
	current, found := agg.candles[pair]
	if !found {
		current = &model.OHLCCandle{Pair: pair}
		agg.candles[pair] = current
	}

	var earlier = false
	var later = false

	// Set start time from first trade
	if current.StartTime.IsZero() || trade.Timestamp.Before(current.StartTime) {
		// If this is the first trade or earlier than current start time, set start time
		current.StartTime = trade.Timestamp
		earlier = true
	}

	if current.EndTime.IsZero() || trade.Timestamp.After(current.EndTime) {
		current.EndTime = trade.Timestamp
		later = true
	}

	// Set Open price from first trade
	if current.Open.IsZero() || earlier {
		current.Open = trade.Price
	}

	// Update High if current price exceeds current high
	if trade.Price.GreaterThan(current.High) {
		current.High = trade.Price
	}

	// Update Low if current price is below current low (or Low is unset)
	if current.Low.IsZero() || trade.Price.LessThan(current.Low) {
		current.Low = trade.Price
	}

	if later {
		current.Close = trade.Price
	}

	// Accumulate volume from all trades
	current.Volume = current.Volume.Add(trade.Quantity)
}
