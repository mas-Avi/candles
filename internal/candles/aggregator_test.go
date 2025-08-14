package candles

import (
	"candles/internal/model"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockExchangeConnector is a mock implementation of ExchangeConnector for testing.
type MockExchangeConnector struct {
	mock.Mock

	// tradeChan delivers trade events to the aggregator
	tradeChan chan model.TradeEvent

	// name identifies this exchange for logging and debugging
	name string

	// closed tracks whether the connector has been closed
	closed bool

	// mu protects concurrent access to connector state
	mu sync.RWMutex
}

// NewMockExchangeConnector creates a new mock exchange connector with specified name.
func NewMockExchangeConnector(name string) *MockExchangeConnector {
	return &MockExchangeConnector{
		tradeChan: make(chan model.TradeEvent, 100),
		name:      name,
	}
}

// SubscribeToTrades implements the ExchangeConnector interface for testing.
func (m *MockExchangeConnector) SubscribeToTrades(ctx context.Context, pairs []string) (<-chan model.TradeEvent, error) {
	args := m.Called(ctx, pairs)

	// Return any configured error from mock expectations
	if err := args.Error(0); err != nil {
		return nil, err
	}

	// Start goroutine to close channel when context is cancelled
	go func() {
		<-ctx.Done()
		m.close()
	}()

	return m.tradeChan, nil
}

// SendTrade sends a trade event through the mock connector's channel.
func (m *MockExchangeConnector) SendTrade(trade model.TradeEvent) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if !m.closed {
		select {
		case m.tradeChan <- trade:
			// Trade sent successfully
		default:
			// Channel full, would block - this is a test configuration issue
			panic(fmt.Sprintf("mock exchange %s channel full", m.name))
		}
	}
}

// SendTrades sends multiple trade events in sequence.
func (m *MockExchangeConnector) SendTrades(trades []model.TradeEvent) {
	for _, trade := range trades {
		m.SendTrade(trade)
	}
}

// SendTradeWithDelay sends a trade event after a specified delay.
func (m *MockExchangeConnector) SendTradeWithDelay(trade model.TradeEvent, delay time.Duration) {
	go func() {
		time.Sleep(delay)
		m.SendTrade(trade)
	}()
}

// close safely closes the trade channel and marks the connector as closed.
func (m *MockExchangeConnector) close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.closed {
		close(m.tradeChan)
		m.closed = true
	}
}

// Helper function to create test trade events with realistic data.
func createTestTrade(pair, price, quantity string, timestamp time.Time, exchange model.Exchange) model.TradeEvent {
	priceDecimal, _ := decimal.NewFromString(price)
	quantityDecimal, _ := decimal.NewFromString(quantity)

	return model.TradeEvent{
		Pair:      pair,
		Price:     priceDecimal,
		Quantity:  quantityDecimal,
		Timestamp: timestamp,
		Exchange:  exchange,
	}
}

// Test_NewAggregator tests the aggregator constructor with various configurations.
func Test_NewAggregator(t *testing.T) {
	tests := []struct {
		name        string
		exchanges   []ExchangeConnector
		interval    time.Duration
		expectValid bool
		description string
	}{
		{
			name:        "Valid configuration with single exchange",
			exchanges:   []ExchangeConnector{NewMockExchangeConnector("binance")},
			interval:    time.Minute,
			expectValid: true,
			description: "Should create aggregator with single exchange connector",
		},
		{
			name: "Valid configuration with multiple exchanges",
			exchanges: []ExchangeConnector{
				NewMockExchangeConnector("binance"),
				NewMockExchangeConnector("okx"),
				NewMockExchangeConnector("coinbase"),
			},
			interval:    5 * time.Minute,
			expectValid: true,
			description: "Should create aggregator with multiple exchange connectors",
		},
		{
			name:        "Valid configuration with short interval",
			exchanges:   []ExchangeConnector{NewMockExchangeConnector("binance")},
			interval:    time.Second,
			expectValid: true,
			description: "Should handle very short aggregation intervals",
		},
		{
			name:        "Valid configuration with long interval",
			exchanges:   []ExchangeConnector{NewMockExchangeConnector("binance")},
			interval:    time.Hour,
			expectValid: true,
			description: "Should handle long aggregation intervals",
		},
		{
			name:        "Empty exchanges slice",
			exchanges:   []ExchangeConnector{},
			interval:    time.Minute,
			expectValid: true, // Constructor doesn't validate this
			description: "Constructor allows empty exchanges (validation happens later)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := NewAggregator(tt.exchanges, tt.interval)

			if tt.expectValid {
				assert.NotNil(t, agg, "Aggregator should be created")
				assert.Equal(t, len(tt.exchanges), len(agg.exchanges), "Should store all exchanges")
				assert.Equal(t, tt.interval, agg.interval, "Should store correct interval")
				assert.NotNil(t, agg.candles, "Should initialize candles map")
				assert.Empty(t, agg.candles, "Candles map should be empty initially")
			}
		})
	}
}

// Test_StartCandleStream_Success tests successful candle stream initialization.
//
// This test verifies that the aggregator can successfully establish subscriptions
// with all configured exchanges and return a working candle stream channel.
func Test_StartCandleStream_Success(t *testing.T) {

	// Create mock exchanges
	exchange1 := NewMockExchangeConnector("binance")
	exchange2 := NewMockExchangeConnector("okx")

	// Configure mock expectations
	pairs := []string{"BTC-USDT", "ETH-USDT"}
	exchange1.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)
	exchange2.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)

	// Create aggregator
	agg := NewAggregator([]ExchangeConnector{exchange1, exchange2}, time.Second)

	// Start candle stream
	ctx := context.Background()
	candleStream, err := agg.StartCandleStream(ctx, pairs)

	// Verify successful initialization
	require.NoError(t, err, "Should start candle stream successfully")
	require.NotNil(t, candleStream, "Should return valid candle channel")

	// Verify channel is receiving (not closed)
	select {
	case _, ok := <-candleStream:
		if !ok {
			t.Error("Candle stream should not be closed immediately")
		}
	case <-time.After(10 * time.Millisecond):
		// Expected - no candles yet
	}

	// Verify mock expectations
	exchange1.AssertExpectations(t)
	exchange2.AssertExpectations(t)
}

// Test_StartCandleStream_ExchangeFailure tests handling of exchange subscription failures.
//
// This test ensures that the aggregator properly handles cases where one or more
// exchanges fail to establish subscriptions, implementing fail-fast behavior.
func Test_StartCandleStream_ExchangeFailure(t *testing.T) {

	// Create mock exchanges
	exchange1 := NewMockExchangeConnector("binance")
	exchange2 := NewMockExchangeConnector("okx")

	// Configure mock expectations - exchange2 fails
	pairs := []string{"BTC-USDT"}
	exchange1.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)
	exchange2.On("SubscribeToTrades", mock.Anything, pairs).Return(fmt.Errorf("connection failed"))

	// Create aggregator
	agg := NewAggregator([]ExchangeConnector{exchange1, exchange2}, time.Second)

	// Start candle stream
	ctx := context.Background()
	candleStream, err := agg.StartCandleStream(ctx, pairs)

	// Verify failure handling
	assert.Error(t, err, "Should return error on exchange failure")
	assert.Nil(t, candleStream, "Should not return channel on failure")
	assert.Contains(t, err.Error(), "failed to subscribe to trades", "Error should indicate subscription failure")

	// Verify mock expectations
	exchange1.AssertExpectations(t)
	exchange2.AssertExpectations(t)
}

// Test_OHLC_Calculation tests the core OHLC calculation logic.
//
// This test verifies that the aggregator correctly calculates Open, High, Low,
// Close, and Volume values from a sequence of trade events.
func Test_OHLC_Calculation(t *testing.T) {

	exchange := NewMockExchangeConnector("test")

	// Configure mock
	pairs := []string{"BTC-USDT"}
	exchange.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)

	// Create aggregator with very short interval for quick testing
	agg := NewAggregator([]ExchangeConnector{exchange}, 100*time.Millisecond)

	// Start candle stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleStream, err := agg.StartCandleStream(ctx, pairs)
	require.NoError(t, err)

	// Define test trades for OHLC calculation
	baseTime := time.Now()
	trades := []model.TradeEvent{
		createTestTrade("BTC-USDT", "50000", "0.1", baseTime, model.BinanceExchange),                     // Open: 50000
		createTestTrade("BTC-USDT", "50500", "0.05", baseTime.Add(time.Second), model.BinanceExchange),   // High: 50500
		createTestTrade("BTC-USDT", "49800", "0.2", baseTime.Add(2*time.Second), model.BinanceExchange),  // Low: 49800
		createTestTrade("BTC-USDT", "50200", "0.15", baseTime.Add(3*time.Second), model.BinanceExchange), // Close: 50200
	}

	// Send trades to aggregator
	for _, trade := range trades {
		exchange.SendTrade(trade)
	}

	// Wait for candle to be published
	select {
	case candle := <-candleStream:
		// Verify OHLC values
		assert.Equal(t, "BTC-USDT", candle.Pair, "Should have correct trading pair")
		assert.Equal(t, "50000", candle.Open.String(), "Open should be first trade price")
		assert.Equal(t, "50500", candle.High.String(), "High should be maximum price")
		assert.Equal(t, "49800", candle.Low.String(), "Low should be minimum price")
		assert.Equal(t, "50200", candle.Close.String(), "Close should be last trade price")
		assert.Equal(t, "0.5", candle.Volume.String(), "Volume should sum all quantities")
		assert.False(t, candle.StartTime.IsZero(), "Should set start time")
		assert.False(t, candle.EndTime.IsZero(), "Should set end time")

	case <-time.After(500 * time.Millisecond):
		t.Fatal("Should receive candle within timeout")
	}
}

// Test_Multiple_Trading_Pairs tests aggregator behavior with multiple trading pairs.
//
// This test ensures that the aggregator maintains separate OHLC state for each
// trading pair and doesn't mix data between different pairs.
func Test_Multiple_Trading_Pairs(t *testing.T) {

	exchange := NewMockExchangeConnector("test")

	// Configure mock
	pairs := []string{"BTC-USDT", "ETH-USDT"}
	exchange.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)

	// Create aggregator
	agg := NewAggregator([]ExchangeConnector{exchange}, 100*time.Millisecond)

	// Start candle stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleStream, err := agg.StartCandleStream(ctx, pairs)
	require.NoError(t, err)

	// Send trades for different pairs
	baseTime := time.Now()
	btcTrades := []model.TradeEvent{
		createTestTrade("BTC-USDT", "50000", "0.1", baseTime, model.BinanceExchange),
		createTestTrade("BTC-USDT", "50100", "0.05", baseTime.Add(time.Second), model.BinanceExchange),
	}
	ethTrades := []model.TradeEvent{
		createTestTrade("ETH-USDT", "3000", "1.0", baseTime, model.BinanceExchange),
		createTestTrade("ETH-USDT", "3050", "0.5", baseTime.Add(time.Second), model.BinanceExchange),
	}

	// Send all trades
	exchange.SendTrades(btcTrades)
	exchange.SendTrades(ethTrades)

	// Collect candles
	receivedCandles := make(map[string]model.OHLCCandle)
	timeout := time.After(500 * time.Millisecond)

	for len(receivedCandles) < 2 {
		select {
		case candle := <-candleStream:
			receivedCandles[candle.Pair] = candle
		case <-timeout:
			t.Fatal("Should receive candles for both pairs")
		}
	}

	// Verify BTC candle
	btcCandle, hasBTC := receivedCandles["BTC-USDT"]
	require.True(t, hasBTC, "Should have BTC candle")
	assert.Equal(t, "50000", btcCandle.Open.String(), "BTC open price")
	assert.Equal(t, "50100", btcCandle.High.String(), "BTC high price")
	assert.Equal(t, "50000", btcCandle.Low.String(), "BTC low price")
	assert.Equal(t, "50100", btcCandle.Close.String(), "BTC close price")
	assert.Equal(t, "0.15", btcCandle.Volume.String(), "BTC volume")

	// Verify ETH candle
	ethCandle, hasETH := receivedCandles["ETH-USDT"]
	require.True(t, hasETH, "Should have ETH candle")
	assert.Equal(t, "3000", ethCandle.Open.String(), "ETH open price")
	assert.Equal(t, "3050", ethCandle.High.String(), "ETH high price")
	assert.Equal(t, "3000", ethCandle.Low.String(), "ETH low price")
	assert.Equal(t, "3050", ethCandle.Close.String(), "ETH close price")
	assert.Equal(t, "1.5", ethCandle.Volume.String(), "ETH volume")
}

// Test_Multiple_Exchanges tests aggregator behavior with multiple exchanges.
//
// This test verifies that trades from different exchanges are properly merged
// and contribute to the same OHLC candles for each trading pair.
func Test_Multiple_Exchanges(t *testing.T) {

	// Create multiple mock exchanges
	binance := NewMockExchangeConnector("binance")
	okx := NewMockExchangeConnector("okx")

	// Configure mocks
	pairs := []string{"BTC-USDT"}
	binance.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)
	okx.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)

	// Create aggregator
	agg := NewAggregator([]ExchangeConnector{binance, okx}, 100*time.Millisecond)

	// Start candle stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleStream, err := agg.StartCandleStream(ctx, pairs)
	require.NoError(t, err)

	// Send trades from different exchanges
	baseTime := time.Now()
	binance.SendTrade(createTestTrade("BTC-USDT", "50000", "0.1", baseTime, model.BinanceExchange))
	okx.SendTrade(createTestTrade("BTC-USDT", "50200", "0.05", baseTime.Add(time.Second), model.OkxExchange))
	binance.SendTrade(createTestTrade("BTC-USDT", "49900", "0.2", baseTime.Add(2*time.Second), model.BinanceExchange))

	// Wait for candle
	select {
	case candle := <-candleStream:
		// Verify aggregated data from multiple exchanges
		assert.Equal(t, "BTC-USDT", candle.Pair)
		assert.Equal(t, "50000", candle.Open.String(), "Open from first trade (Binance)")
		assert.Equal(t, "50200", candle.High.String(), "High from OKX trade")
		assert.Equal(t, "49900", candle.Low.String(), "Low from second Binance trade")
		assert.Equal(t, "49900", candle.Close.String(), "Close from last trade")
		assert.Equal(t, "0.35", candle.Volume.String(), "Volume from all exchanges")

	case <-time.After(500 * time.Millisecond):
		t.Fatal("Should receive aggregated candle")
	}
}

// Test_Empty_Candle_Handling tests behavior when no trades occur during an interval.
//
// This test ensures that the aggregator properly handles intervals with no trade
// activity and doesn't publish empty candles.
func Test_Empty_Candle_Handling(t *testing.T) {

	exchange := NewMockExchangeConnector("test")

	// Configure mock
	pairs := []string{"BTC-USDT"}
	exchange.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)

	// Create aggregator with short interval
	agg := NewAggregator([]ExchangeConnector{exchange}, 50*time.Millisecond)

	// Start candle stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleStream, err := agg.StartCandleStream(ctx, pairs)
	require.NoError(t, err)

	// Don't send any trades, wait for intervals to pass
	select {
	case candle := <-candleStream:
		t.Errorf("Should not receive empty candle: %+v", candle)
	case <-time.After(200 * time.Millisecond): // Wait for multiple intervals
		// Expected - no candles should be published
	}
}

// Test_Candle_Reset_Between_Intervals tests candle state reset between intervals.
//
// This test verifies that candle data is properly reset between intervals and
// doesn't carry over state from previous periods.
func Test_Candle_Reset_Between_Intervals(t *testing.T) {

	exchange := NewMockExchangeConnector("test")

	// Configure mock
	pairs := []string{"BTC-USDT"}
	exchange.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)

	// Create aggregator with short interval
	agg := NewAggregator([]ExchangeConnector{exchange}, 100*time.Millisecond)

	// Start candle stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleStream, err := agg.StartCandleStream(ctx, pairs)
	require.NoError(t, err)

	baseTime := time.Now()

	// Send trades for first interval
	exchange.SendTrade(createTestTrade("BTC-USDT", "50000", "0.1", baseTime, model.BinanceExchange))
	exchange.SendTrade(createTestTrade("BTC-USDT", "50500", "0.05", baseTime.Add(10*time.Millisecond), model.BinanceExchange))

	// Receive first candle
	var firstCandle model.OHLCCandle
	select {
	case firstCandle = <-candleStream:
		assert.Equal(t, "50000", firstCandle.Open.String(), "First candle open")
		assert.Equal(t, "50500", firstCandle.High.String(), "First candle high")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Should receive first candle")
	}

	// Send trades for second interval with different price range
	exchange.SendTrade(createTestTrade("BTC-USDT", "49000", "0.2", baseTime.Add(150*time.Millisecond), model.BinanceExchange))
	exchange.SendTrade(createTestTrade("BTC-USDT", "49200", "0.1", baseTime.Add(160*time.Millisecond), model.BinanceExchange))

	// Receive second candle
	select {
	case secondCandle := <-candleStream:
		// Verify second candle is independent of first
		assert.Equal(t, "49000", secondCandle.Open.String(), "Second candle open should be reset")
		assert.Equal(t, "49200", secondCandle.High.String(), "Second candle high should be reset")
		assert.Equal(t, "49000", secondCandle.Low.String(), "Second candle low should be reset")
		assert.Equal(t, "0.3", secondCandle.Volume.String(), "Second candle volume should be reset")

		// Verify candles are independent
		assert.NotEqual(t, firstCandle.Open.String(), secondCandle.Open.String(), "Candles should have different opens")
		assert.NotEqual(t, firstCandle.High.String(), secondCandle.High.String(), "Candles should have different highs")

	case <-time.After(200 * time.Millisecond):
		t.Fatal("Should receive second candle")
	}
}

// Test_Context_Cancellation tests proper shutdown on context cancellation.
//
// This test ensures that the aggregator gracefully shuts down when the context
// is cancelled and properly closes all channels and cleans up resources.
func Test_Context_Cancellation(t *testing.T) {

	exchange := NewMockExchangeConnector("test")

	// Configure mock
	pairs := []string{"BTC-USDT"}
	exchange.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)

	// Create aggregator
	agg := NewAggregator([]ExchangeConnector{exchange}, time.Minute)

	// Start candle stream with cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	candleStream, err := agg.StartCandleStream(ctx, pairs)
	require.NoError(t, err)

	// Send a trade
	exchange.SendTrade(createTestTrade("BTC-USDT", "50000", "0.1", time.Now(), model.BinanceExchange))

	// Cancel context
	cancel()

	// Verify channel is closed
	select {
	case _, ok := <-candleStream:
		if ok {
			t.Error("Channel should be closed after context cancellation")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Channel should be closed quickly after context cancellation")
	}
}

// Test_High_Frequency_Trading tests aggregator performance with high trade volume.
//
// This test simulates high-frequency trading scenarios to ensure the aggregator
// can handle large volumes of trade events without dropping data or performance issues.
func Test_High_Frequency_Trading(t *testing.T) {

	exchange := NewMockExchangeConnector("test")

	// Configure mock
	pairs := []string{"BTC-USDT"}
	exchange.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)

	// Create aggregator
	agg := NewAggregator([]ExchangeConnector{exchange}, 800*time.Millisecond)

	// Start candle stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleStream, err := agg.StartCandleStream(ctx, pairs)
	require.NoError(t, err)

	// Generate high-frequency trades
	baseTime := time.Now()
	tradeCount := 1000
	basePrice := decimal.NewFromInt(50000)
	totalVolume := decimal.Zero

	// Send many trades quickly
	go func() {
		for i := 0; i < tradeCount; i++ {
			// Vary price slightly for realistic OHLC data
			priceVariation := decimal.NewFromInt(int64(i%200 - 100)) // -100 to +99
			price := basePrice.Add(priceVariation)
			quantity := decimal.NewFromFloat(0.001)

			trade := model.TradeEvent{
				Pair:      "BTC-USDT",
				Price:     price,
				Quantity:  quantity,
				Timestamp: baseTime.Add(time.Duration(i) * time.Microsecond),
				Exchange:  model.BinanceExchange,
			}

			exchange.SendTrade(trade)
			totalVolume = totalVolume.Add(quantity)
		}

		// Close down the aggregator after sending all trades
		cancel()
	}()

	// Wait for candle
	for {
		select {
		case candle, ok := <-candleStream:
			if !ok {
				return
			}
			// Verify all trades were processed
			expectedVolume := decimal.NewFromFloat(0.001).Mul(decimal.NewFromInt(int64(tradeCount)))
			assert.Equal(t, expectedVolume.String(), candle.Volume.String(), "Should process all high-frequency trades")
			assert.Equal(t, "BTC-USDT", candle.Pair, "Should maintain correct pair")
			assert.False(t, candle.Open.IsZero(), "Should have valid open price")
			assert.False(t, candle.High.IsZero(), "Should have valid high price")
			assert.False(t, candle.Low.IsZero(), "Should have valid low price")
			assert.False(t, candle.Close.IsZero(), "Should have valid close price")

		case <-time.After(1 * time.Second):
			t.Fatal("Should process high-frequency trades within timeout")
		}
	}
}

// Test_Precision_Handling tests decimal precision handling in OHLC calculations.
//
// This test ensures that the aggregator maintains full precision throughout
// the calculation process and doesn't introduce rounding errors.
func Test_Precision_Handling(t *testing.T) {

	exchange := NewMockExchangeConnector("test")

	// Configure mock
	pairs := []string{"BTC-USDT"}
	exchange.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)

	// Create aggregator
	agg := NewAggregator([]ExchangeConnector{exchange}, 100*time.Millisecond)

	// Start candle stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleStream, err := agg.StartCandleStream(ctx, pairs)
	require.NoError(t, err)

	// Send trades with high precision values
	baseTime := time.Now()
	trades := []model.TradeEvent{
		createTestTrade("BTC-USDT", "50000.12345678", "0.00000001", baseTime, model.BinanceExchange),
		createTestTrade("BTC-USDT", "50000.87654321", "0.00000002", baseTime.Add(time.Second), model.BinanceExchange),
		createTestTrade("BTC-USDT", "49999.99999999", "0.00000003", baseTime.Add(2*time.Second), model.BinanceExchange),
	}

	exchange.SendTrades(trades)

	// Wait for candle
	select {
	case candle := <-candleStream:
		// Verify precision is maintained
		assert.Equal(t, "50000.12345678", candle.Open.String(), "Should maintain open price precision")
		assert.Equal(t, "50000.87654321", candle.High.String(), "Should maintain high price precision")
		assert.Equal(t, "49999.99999999", candle.Low.String(), "Should maintain low price precision")
		assert.Equal(t, "49999.99999999", candle.Close.String(), "Should maintain close price precision")
		assert.Equal(t, "0.00000006", candle.Volume.String(), "Should maintain volume precision")

	case <-time.After(200 * time.Millisecond):
		t.Fatal("Should receive precision test candle")
	}
}

// Test_Concurrent_Access tests thread safety with concurrent operations.
//
// This test ensures that the aggregator handles concurrent trade events from
// multiple exchanges safely without race conditions or data corruption.
func Test_Concurrent_Access(t *testing.T) {

	// Create multiple exchanges
	exchanges := make([]ExchangeConnector, 5)
	mocks := make([]*MockExchangeConnector, 5)
	for i := 0; i < 5; i++ {
		mock := NewMockExchangeConnector(fmt.Sprintf("exchange%d", i))
		exchanges[i] = mock
		mocks[i] = mock
	}

	// Configure mocks
	pairs := []string{"BTC-USDT"}
	for _, mockExchangeConnector := range mocks {
		mockExchangeConnector.On("SubscribeToTrades", mock.Anything, pairs).Return(nil)
	}

	// Create aggregator
	agg := NewAggregator(exchanges, 200*time.Millisecond)

	// Start candle stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleStream, err := agg.StartCandleStream(ctx, pairs)
	require.NoError(t, err)

	// Send concurrent trades from all exchanges
	var wg sync.WaitGroup
	tradesPerExchange := 100
	baseTime := time.Now()

	for i, mock := range mocks {
		wg.Add(1)
		go func(exchangeIndex int, exchange *MockExchangeConnector) {
			defer wg.Done()
			for j := 0; j < tradesPerExchange; j++ {
				price := fmt.Sprintf("%d", 50000+exchangeIndex*100+j)
				quantity := "0.001"
				trade := createTestTrade("BTC-USDT", price, quantity,
					baseTime.Add(time.Duration(j)*time.Microsecond), model.BinanceExchange)
				exchange.SendTrade(trade)
			}
		}(i, mock)
	}

	// Wait for all trades to be sent
	wg.Wait()

	// Wait for candle
	select {
	case candle := <-candleStream:
		// Verify all trades were processed correctly
		expectedVolume := decimal.NewFromFloat(0.001).Mul(decimal.NewFromInt(int64(len(exchanges) * tradesPerExchange)))
		assert.Equal(t, expectedVolume.String(), candle.Volume.String(), "Should process all concurrent trades")
		assert.Equal(t, "BTC-USDT", candle.Pair, "Should maintain correct pair")
		assert.False(t, candle.Open.IsZero(), "Should have valid OHLC data")

	case <-time.After(1 * time.Second):
		t.Fatal("Should process concurrent trades within timeout")
	}
}

// Benchmark_OHLC_Calculation benchmarks the core OHLC calculation performance.
//
// This benchmark measures the performance of trade processing and OHLC updates
// to ensure the aggregator can handle high-frequency trading requirements.
func Benchmark_OHLC_Calculation(b *testing.B) {

	agg := NewAggregator(nil, time.Minute)

	// Prepare test trade
	trade := createTestTrade("BTC-USDT", "50000.12345678", "0.001", time.Now(), model.BinanceExchange)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		agg.updateCandle(trade)
	}
}

// Benchmark_FanIn_Performance benchmarks the fan-in operation performance.
//
// This benchmark measures the performance of merging multiple trade streams
// to ensure efficient handling of high-frequency data from multiple exchanges.
func Benchmark_FanIn_Performance(b *testing.B) {
	agg := NewAggregator(nil, time.Minute)

	// Create multiple input channels
	numChannels := 10
	inputChannels := make([]<-chan model.TradeEvent, numChannels)
	senders := make([]chan model.TradeEvent, numChannels)

	for i := 0; i < numChannels; i++ {
		ch := make(chan model.TradeEvent, 1000)
		inputChannels[i] = ch
		senders[i] = ch
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start fan-in
	output := agg.fanIn(ctx, inputChannels)

	// Prepare test trade
	trade := createTestTrade("BTC-USDT", "50000", "0.001", time.Now(), model.BinanceExchange)

	b.ResetTimer()
	b.ReportAllocs()

	go func() {
		for {
			select {
			case <-output:
				// Consume output
			case <-ctx.Done():
				return
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		senders[i%numChannels] <- trade
	}

	// Clean up
	for _, sender := range senders {
		close(sender)
	}
}
