// Package service provides core business logic components for the candle streaming service.
//
// The CandleService acts as the main orchestrator that coordinates between candle data
// aggregation and distribution to multiple subscribers via gRPC streaming. It implements
// a robust real-time streaming system with proper lifecycle management and error handling.
package service

import (
	pb "candles/gen/proto"
	"candles/internal/model"
	"candles/internal/utils"
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/rs/zerolog/log"
)

// TradeCandleAggregator defines the interface for components that aggregate
// raw trading data into candle/OHLCV format.
type TradeCandleAggregator interface {
	// StartCandleStream initiates candle data aggregation for the specified trading pairs.
	StartCandleStream(ctx context.Context, pairs []string) (<-chan model.OHLCCandle, error)
}

// SubscriptionManager defines the interface for managing client subscriptions
// and distributing candle data to multiple subscribers.
type SubscriptionManager interface {
	// Subscribe creates a new subscription for the specified trading pairs.
	Subscribe(pairs []string) (*Subscriber, error)

	// Unsubscribe removes a subscriber and cleans up associated resources.
	Unsubscribe(sub *Subscriber) error

	// StartDispatching begins the message distribution process.
	StartDispatching(ctx context.Context, ch <-chan model.OHLCCandle) error
}

// CandleService implements the gRPC CandleService interface and orchestrates
// the entire candle streaming system.
//
// The service coordinates between:
//   - TradeCandleAggregator: Provides raw candle data
//   - SubscriptionManager: Manages client subscriptions and distribution
//   - gRPC clients: Streams candle data via Subscribe RPC
type CandleService struct {
	pb.UnimplementedCandleServiceServer                       // Embed unimplemented server for forward compatibility
	subscriptionManager                 SubscriptionManager   // Handles client subscription lifecycle
	candleAggregator                    TradeCandleAggregator // Provides candle data stream
	started                             atomic.Bool           // Atomic flag tracking service state
	cancel                              context.CancelFunc    // Function to cancel service context
}

// NewCandleService creates a new CandleService instance with the provided dependencies.
//
// The service is created in a stopped state and must be started with the Start method
// before it can accept client subscriptions.
func NewCandleService(manager SubscriptionManager, aggregator TradeCandleAggregator) *CandleService {
	return &CandleService{
		subscriptionManager: manager,
		candleAggregator:    aggregator,
	}
}

// Start initializes and starts the candle streaming service for the specified trading pairs.
func (cs *CandleService) Start(ctx context.Context, pairs []string) error {
	if !cs.started.CompareAndSwap(false, true) {
		return errors.New("candle service has already started")
	}

	ctx, cancel := context.WithCancel(ctx)

	// Start receiving candles from the aggregator
	candleChan, err := cs.candleAggregator.StartCandleStream(ctx, pairs)
	if err != nil {
		cs.started.Store(false)
		return fmt.Errorf("failed to start aggregator: %v", err)
	}

	// Forward candles to subscribers
	err = cs.subscriptionManager.StartDispatching(ctx, candleChan)
	if err != nil {
		// cancel context so candle stream stops
		cancel()
		cs.started.Store(false)
		return fmt.Errorf("failed to start dispatching: %v", err)
	}

	cs.cancel = cancel
	return nil
}

// Stop gracefully shuts down the candle service.
func (cs *CandleService) Stop() error {
	if !cs.started.CompareAndSwap(true, false) {
		return errors.New("service not started")
	}

	if cs.cancel != nil {
		cs.cancel()
		cs.cancel = nil
	}

	log.Info().Msg("CandleService stopped")
	return nil
}

// Subscribe implements the CandleService gRPC Subscribe RPC method.
//
// This method handles client subscription requests and streams real-time candle data
// for the requested trading pairs. It performs comprehensive validation and implements
// graceful error handling.
func (cs *CandleService) Subscribe(req *pb.SubscriptionRequest, stream pb.CandleService_SubscribeServer) error {
	if !cs.started.Load() {
		return errors.New("candle service not started")
	}

	// Validate input parameters
	if req == nil {
		return errors.New("request cannot be nil")
	}

	if len(req.Symbols) == 0 {
		return errors.New("no symbols provided")
	}

	// Validate each trading pair symbol
	for i, symbol := range req.Symbols {
		if err := utils.ValidateSymbol(symbol); err != nil {
			return fmt.Errorf("invalid symbol at index %d (%q): %w", i, symbol, err)
		}
	}

	// Create subscription
	sub, err := cs.subscriptionManager.Subscribe(req.Symbols)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %v", err)
	}

	// Ensure cleanup on method exit
	defer func() {
		if err := cs.subscriptionManager.Unsubscribe(sub); err != nil {
			log.Error().Err(err).Strs("symbols", req.Symbols).Msg("failed to unsubscribe")
		}
	}()

	log.Info().Strs("symbols", req.Symbols).Msg("new client subscription")

	// Stream candle data until client disconnects or error occurs
	for {
		select {
		case <-stream.Context().Done():
			log.Info().Strs("symbols", req.Symbols).Msg("client disconnected")
			return nil
		case tick, ok := <-sub.ch:
			if !ok {
				log.Info().Strs("symbols", req.Symbols).Msg("subscription channel closed")
				return nil
			}

			if err := stream.Send(toCandle(tick)); err != nil {
				log.Error().Err(err).Strs("symbols", req.Symbols).Msg("failed to send candle to client")
				return fmt.Errorf("failed to send candle: %w", err)
			}
		}
	}
}

func toCandle(c model.OHLCCandle) *pb.Candle {
	return &pb.Candle{
		Symbol:         c.Pair,
		Open:           fmt.Sprintf("%s", c.Open.String()),
		Close:          fmt.Sprintf("%s", c.Close.String()),
		High:           fmt.Sprintf("%s", c.High.String()),
		Low:            fmt.Sprintf("%s", c.Low.String()),
		Volume:         fmt.Sprintf("%s", c.Volume.String()),
		StartTimestamp: fmt.Sprintf("%d", c.StartTime.UnixMilli()),
		EndTimestamp:   fmt.Sprintf("%d", c.EndTime.UnixMilli()),
	}
}
