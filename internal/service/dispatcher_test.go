package service

import (
	"candles/internal/model"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestConfig creates a standard test configuration
func createTestConfig() DispatcherConfig {
	return DispatcherConfig{
		MaxSymbolsAllowed: 2,
	}
}

// createTestCandle creates a test candle with specified pair
func createTestCandle(pair string, price float64) model.OHLCCandle {
	return model.OHLCCandle{
		Pair:      pair,
		Open:      decimal.NewFromFloat(price),
		High:      decimal.NewFromFloat(price + 1),
		Low:       decimal.NewFromFloat(price - 1),
		Close:     decimal.NewFromFloat(price),
		Volume:    decimal.NewFromFloat(100),
		StartTime: time.Now(),
		EndTime:   time.Now(),
	}
}

// Test_NewDispatcher tests the dispatcher constructor
func Test_NewDispatcher(t *testing.T) {
	tests := []struct {
		name        string
		config      DispatcherConfig
		description string
	}{
		{
			name:        "Valid configuration",
			config:      DispatcherConfig{MaxSymbolsAllowed: 10},
			description: "Should create dispatcher with valid configuration",
		},
		{
			name:        "Zero max symbols",
			config:      DispatcherConfig{MaxSymbolsAllowed: 0},
			description: "Should create dispatcher with zero max symbols",
		},
		{
			name:        "Large max symbols",
			config:      DispatcherConfig{MaxSymbolsAllowed: 1000},
			description: "Should create dispatcher with large max symbols",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dispatcher := NewDispatcher(tt.config)

			assert.NotNil(t, dispatcher, tt.description)
			assert.Equal(t, tt.config, dispatcher.cfg, "Should store configuration correctly")
			assert.NotNil(t, dispatcher.subscribers, "Should initialize subscribers map")
			assert.NotNil(t, dispatcher.subscriptionCh, "Should initialize subscription channel")
			assert.NotNil(t, dispatcher.unsubscriptionCh, "Should initialize unsubscription channel")
			assert.False(t, dispatcher.started.Load(), "Should start in stopped state")

			// Verify channel capacity
			assert.Equal(t, 10, cap(dispatcher.subscriptionCh), "Should have buffered subscription channel")
			assert.Equal(t, 10, cap(dispatcher.unsubscriptionCh), "Should have buffered unsubscription channel")
		})
	}
}

// Test_StartDispatching tests the dispatcher startup functionality
func Test_StartDispatching(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func(*Dispatcher)
		expectError bool
		description string
	}{
		{
			name:        "Start new dispatcher",
			setupFunc:   func(d *Dispatcher) {},
			expectError: false,
			description: "Should start new dispatcher successfully",
		},
		{
			name: "Start already started dispatcher",
			setupFunc: func(d *Dispatcher) {
				d.started.Store(true) // Simulate already started
			},
			expectError: true,
			description: "Should reject starting already started dispatcher",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dispatcher := NewDispatcher(createTestConfig())
			tt.setupFunc(dispatcher)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			candleCh := make(chan model.OHLCCandle, 10)
			defer close(candleCh)

			err := dispatcher.StartDispatching(ctx, candleCh)

			if tt.expectError {
				assert.Error(t, err, tt.description)
				assert.Contains(t, err.Error(), "already started", "Error should mention already started")
			} else {
				assert.NoError(t, err, tt.description)
				assert.True(t, dispatcher.started.Load(), "Should set started flag")

				// Give dispatcher time to start
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

// Test_Subscribe tests subscription functionality
func Test_Subscribe(t *testing.T) {
	tests := []struct {
		name          string
		pairs         []string
		startDispatch bool
		expectError   bool
		errorContains string
		description   string
	}{
		{
			name:          "Valid subscription",
			pairs:         []string{"BTC-USDT", "ETH-USDT"},
			startDispatch: true,
			expectError:   false,
			description:   "Should create subscription for valid pairs",
		},
		{
			name:          "Single pair subscription",
			pairs:         []string{"BTC-USDT"},
			startDispatch: true,
			expectError:   false,
			description:   "Should create subscription for single pair",
		},
		{
			name:          "Maximum allowed pairs",
			pairs:         []string{"BTC-USDT", "ETH-USDT"},
			startDispatch: true,
			expectError:   false,
			description:   "Should create subscription for maximum allowed pairs",
		},
		{
			name:          "Dispatcher not started",
			pairs:         []string{"BTC-USDT"},
			startDispatch: false,
			expectError:   true,
			errorContains: "not started",
			description:   "Should reject subscription when dispatcher not started",
		},
		{
			name:          "Too many pairs",
			pairs:         []string{"BTC-USDT", "ETH-USDT", "ADA-USDT", "DOT-USDT", "SOL-USDT", "LINK-USDT", "UNI-USDT", "AAVE-USDT", "COMP-USDT", "MKR-USDT", "EXTRA-USDT"},
			startDispatch: true,
			expectError:   true,
			errorContains: "too many",
			description:   "Should reject subscription with too many pairs",
		},
		{
			name:          "Empty pairs list",
			pairs:         []string{},
			startDispatch: true,
			expectError:   true,
			errorContains: "zero symbols requested",
			description:   "Should reject empty pairs list",
		},
		{
			name:          "Invalid pair format",
			pairs:         []string{"INVALID"},
			startDispatch: true,
			expectError:   true,
			errorContains: "invalid",
			description:   "Should reject invalid pair format",
		},
		{
			name:          "Nil pairs",
			pairs:         nil,
			startDispatch: true,
			expectError:   true,
			errorContains: "zero symbols requested",
			description:   "Should reject nil pairs",
		},
		{
			name:          "Mixed valid and invalid pairs",
			pairs:         []string{"BTC-USDT", "INVALID"},
			startDispatch: true,
			expectError:   true,
			errorContains: "invalid",
			description:   "Should reject if any pair is invalid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dispatcher := NewDispatcher(createTestConfig())

			if tt.startDispatch {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				candleCh := make(chan model.OHLCCandle, 10)
				defer close(candleCh)

				err := dispatcher.StartDispatching(ctx, candleCh)
				require.NoError(t, err, "Should start dispatcher")

				// Give dispatcher time to start
				time.Sleep(10 * time.Millisecond)
			}

			sub, err := dispatcher.Subscribe(tt.pairs)

			if tt.expectError {
				assert.Error(t, err, tt.description)
				assert.Nil(t, sub, "Should not return subscriber on error")
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains, "Error should contain expected text")
				}
			} else {
				assert.NoError(t, err, tt.description)
				assert.NotNil(t, sub, "Should return valid subscriber")

				if sub != nil {
					assert.NotNil(t, sub.ch, "Should have subscriber channel")
					assert.Equal(t, 100, cap(sub.ch), "Should have correct channel capacity")
					assert.Equal(t, len(tt.pairs), len(sub.symbolsSubscribed), "Should have correct number of subscribed symbols")

					// Verify symbols are correctly stored
					for _, pair := range tt.pairs {
						_, exists := sub.symbolsSubscribed[pair]
						assert.True(t, exists, "Should contain subscribed symbol: %s", pair)
					}

					// Give time for subscription to be processed
					time.Sleep(10 * time.Millisecond)
				}
			}
		})
	}
}

// Test_Unsubscribe tests unsubscription functionality
func Test_Unsubscribe(t *testing.T) {
	dispatcher := NewDispatcher(createTestConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleCh := make(chan model.OHLCCandle, 10)
	defer close(candleCh)

	err := dispatcher.StartDispatching(ctx, candleCh)
	require.NoError(t, err, "Should start dispatcher")

	// Give dispatcher time to start
	time.Sleep(10 * time.Millisecond)

	// Create subscription
	sub, err := dispatcher.Subscribe([]string{"BTC-USDT"})
	require.NoError(t, err, "Should create subscription")
	require.NotNil(t, sub, "Should return valid subscriber")

	// Give time for subscription to be processed
	time.Sleep(10 * time.Millisecond)

	// Test unsubscribe
	err = dispatcher.Unsubscribe(sub)
	assert.NoError(t, err, "Should unsubscribe successfully")

	// Give time for unsubscription to be processed
	time.Sleep(10 * time.Millisecond)

	// Verify channel is closed
	select {
	case _, ok := <-sub.ch:
		assert.False(t, ok, "Subscriber channel should be closed after unsubscribe")
	case <-time.After(100 * time.Millisecond):
		t.Error("Channel should be closed within timeout")
	}
}

// Test_ChannelFullScenarios tests behavior when channels are full
func Test_ChannelFullScenarios(t *testing.T) {
	t.Run("Subscription channel full", func(t *testing.T) {
		dispatcher := NewDispatcher(createTestConfig())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		candleCh := make(chan model.OHLCCandle, 10)
		defer close(candleCh)

		err := dispatcher.StartDispatching(ctx, candleCh)
		require.NoError(t, err, "Should start dispatcher")

		// Give dispatcher time to start
		time.Sleep(10 * time.Millisecond)

		// Fill subscription channel by not processing messages
		// We need to stop the dispatcher goroutine from processing
		cancel() // Stop dispatcher
		time.Sleep(10 * time.Millisecond)

		// Create new dispatcher for this test
		dispatcher2 := NewDispatcher(createTestConfig())
		dispatcher2.started.Store(true) // Mark as started without starting goroutine

		// Fill the subscription channel
		for i := 0; i < 11; i++ { // One more than capacity
			sub := &Subscriber{
				ch:                make(chan model.OHLCCandle, 100),
				symbolsSubscribed: map[string]struct{}{"BTC-USDT": {}},
			}

			select {
			case dispatcher2.subscriptionCh <- sub:
				// Successfully added to channel
			default:
				// Channel is full, this is expected for the last iteration
				if i < 10 {
					t.Errorf("Channel should not be full at iteration %d", i)
				}
			}
		}

		// Now try to subscribe - should fail
		_, err = dispatcher2.Subscribe([]string{"BTC-USDT"})
		assert.Error(t, err, "Should fail when subscription channel is full")
		assert.Contains(t, err.Error(), "subscription channel is full", "Error should mention full channel")
	})

	t.Run("Unsubscription channel full", func(t *testing.T) {
		dispatcher := NewDispatcher(createTestConfig())
		dispatcher.started.Store(true) // Mark as started without starting goroutine

		// Fill the unsubscription channel
		for i := 0; i < 10; i++ {
			sub := &Subscriber{
				ch:                make(chan model.OHLCCandle, 100),
				symbolsSubscribed: map[string]struct{}{"BTC-USDT": {}},
			}
			dispatcher.unsubscriptionCh <- sub
		}

		// Create a subscriber to unsubscribe
		sub := &Subscriber{
			ch:                make(chan model.OHLCCandle, 100),
			symbolsSubscribed: map[string]struct{}{"BTC-USDT": {}},
		}

		// Try to unsubscribe - should fail due to full channel
		err := dispatcher.Unsubscribe(sub)
		assert.Error(t, err, "Should fail when unsubscription channel is full")
		assert.Contains(t, err.Error(), "subscription channel is full", "Error should mention full channel")
	})
}

// Test_MessageDistribution tests candle distribution to subscribers
func Test_MessageDistribution(t *testing.T) {
	dispatcher := NewDispatcher(createTestConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleCh := make(chan model.OHLCCandle, 10)

	err := dispatcher.StartDispatching(ctx, candleCh)
	require.NoError(t, err, "Should start dispatcher")

	// Give dispatcher time to start
	time.Sleep(10 * time.Millisecond)

	// Create multiple subscribers
	sub1, err := dispatcher.Subscribe([]string{"BTC-USDT", "ETH-USDT"})
	require.NoError(t, err, "Should create subscriber 1")

	sub2, err := dispatcher.Subscribe([]string{"BTC-USDT"})
	require.NoError(t, err, "Should create subscriber 2")

	sub3, err := dispatcher.Subscribe([]string{"ETH-USDT"})
	require.NoError(t, err, "Should create subscriber 3")

	// Give time for subscriptions to be processed
	time.Sleep(10 * time.Millisecond)

	tests := []struct {
		name              string
		candle            model.OHLCCandle
		expectedReceivers []*Subscriber
		description       string
	}{
		{
			name:              "BTC-USDT candle",
			candle:            createTestCandle("BTC-USDT", 50000),
			expectedReceivers: []*Subscriber{sub1, sub2},
			description:       "Should deliver BTC-USDT candle to subscribers 1 and 2",
		},
		{
			name:              "ETH-USDT candle",
			candle:            createTestCandle("ETH-USDT", 3000),
			expectedReceivers: []*Subscriber{sub1, sub3},
			description:       "Should deliver ETH-USDT candle to subscriber 1 and 3",
		},
		{
			name:              "Unsubscribed pair",
			candle:            createTestCandle("SOL-USDT", 100),
			expectedReceivers: []*Subscriber{},
			description:       "Should not deliver unsubscribed pair to any subscriber",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Send candle
			candleCh <- tt.candle

			// Give time for distribution
			time.Sleep(10 * time.Millisecond)

			// Check expected receivers
			allSubs := []*Subscriber{sub1, sub2, sub3}
			for _, sub := range allSubs {
				shouldReceive := false
				for _, expectedSub := range tt.expectedReceivers {
					if sub == expectedSub {
						shouldReceive = true
						break
					}
				}

				if shouldReceive {
					select {
					case receivedCandle := <-sub.ch:
						assert.Equal(t, tt.candle.Pair, receivedCandle.Pair, "Should receive correct candle pair")
						assert.True(t, tt.candle.Close.Equal(receivedCandle.Close), "Should receive correct candle price")
					case <-time.After(100 * time.Millisecond):
						t.Errorf("Subscriber should have received candle within timeout")
					}
				} else {
					select {
					case unexpectedCandle := <-sub.ch:
						t.Errorf("Subscriber should not have received candle: %+v", unexpectedCandle)
					default:
						// Expected - no candle received
					}
				}
			}
		})
	}

	close(candleCh)
}

// Test_SlowClientHandling tests behavior with slow clients
func Test_SlowClientHandling(t *testing.T) {
	dispatcher := NewDispatcher(createTestConfig())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleCh := make(chan model.OHLCCandle, 10)
	defer close(candleCh)

	err := dispatcher.StartDispatching(ctx, candleCh)
	require.NoError(t, err, "Should start dispatcher")

	// Give dispatcher time to start
	time.Sleep(10 * time.Millisecond)

	// Create subscriber
	sub, err := dispatcher.Subscribe([]string{"BTC-USDT"})
	require.NoError(t, err, "Should create subscriber")

	// Give time for subscription to be processed
	time.Sleep(10 * time.Millisecond)

	// Fill subscriber buffer by sending many candles without reading
	for i := 0; i < 150; i++ { // More than buffer capacity (100)
		candle := createTestCandle("BTC-USDT", float64(50000+i))
		candleCh <- candle
	}

	// Give time for distribution and buffer management
	time.Sleep(50 * time.Millisecond)

	// Verify that subscriber channel is not blocked and has expected number of items
	channelLength := len(sub.ch)
	assert.Equal(t, 100, channelLength, "Subscriber channel should be at capacity")

	// Read all items from channel to verify oldest were dropped
	receivedCandles := make([]model.OHLCCandle, 0, 100)
	for len(sub.ch) > 0 {
		select {
		case candle := <-sub.ch:
			receivedCandles = append(receivedCandles, candle)
		default:
			break
		}
	}

	assert.Equal(t, 100, len(receivedCandles), "Should receive exactly buffer capacity worth of candles")

	// Verify we got the latest candles (oldest should have been dropped)
	firstCandle := receivedCandles[0]
	lastCandle := receivedCandles[len(receivedCandles)-1]

	// The exact candles we receive depend on timing, but the last should be recent
	assert.True(t, lastCandle.Close.GreaterThan(firstCandle.Close), "Last candle should have higher price than first (newer)")
}

// Test_ConcurrentSubscriptions tests concurrent subscription operations
func Test_ConcurrentSubscriptions(t *testing.T) {
	dispatcher := NewDispatcher(DispatcherConfig{MaxSymbolsAllowed: 100})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleCh := make(chan model.OHLCCandle, 100)
	defer close(candleCh)

	err := dispatcher.StartDispatching(ctx, candleCh)
	require.NoError(t, err, "Should start dispatcher")

	// Give dispatcher time to start
	time.Sleep(10 * time.Millisecond)

	numWorkers := 10
	subscriptionsPerWorker := 5

	var wg sync.WaitGroup
	var successfulSubs int64
	var failedSubs int64

	// Concurrent subscription creation
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			pairs := []string{"BTC-USDT", "SOL-USDT"}
			for i := 0; i < subscriptionsPerWorker; i++ {
				sub, err := dispatcher.Subscribe(pairs)

				if err != nil {
					atomic.AddInt64(&failedSubs, 1)
					t.Logf("Worker %d subscription %d failed: %v", workerID, i, err)
				} else {
					atomic.AddInt64(&successfulSubs, 1)
					// Immediately unsubscribe to test cleanup
					err = dispatcher.Unsubscribe(sub)
					if err != nil {
						t.Logf("Worker %d unsubscription %d failed: %v", workerID, i, err)
					}
				}
			}
		}(w)
	}

	wg.Wait()

	totalExpected := int64(numWorkers * subscriptionsPerWorker)
	totalActual := successfulSubs + failedSubs

	assert.Equal(t, totalExpected, totalActual, "Should account for all subscription attempts")
	assert.Greater(t, successfulSubs, int64(0), "Should have some successful subscriptions")

	// Give time for cleanup
	time.Sleep(50 * time.Millisecond)
}

// Test_DispatcherShutdown tests graceful shutdown behavior
func Test_DispatcherShutdown(t *testing.T) {
	dispatcher := NewDispatcher(createTestConfig())

	ctx, cancel := context.WithCancel(context.Background())

	candleCh := make(chan model.OHLCCandle, 10)

	err := dispatcher.StartDispatching(ctx, candleCh)
	require.NoError(t, err, "Should start dispatcher")

	// Give dispatcher time to start
	time.Sleep(10 * time.Millisecond)

	// Create subscribers
	sub1, err := dispatcher.Subscribe([]string{"BTC-USDT"})
	require.NoError(t, err, "Should create subscriber 1")

	sub2, err := dispatcher.Subscribe([]string{"ETH-USDT"})
	require.NoError(t, err, "Should create subscriber 2")

	// Give time for subscriptions to be processed
	time.Sleep(10 * time.Millisecond)

	// Trigger shutdown
	cancel()
	close(candleCh)

	// Give time for shutdown
	time.Sleep(50 * time.Millisecond)

	// Verify all subscriber channels are closed
	select {
	case _, ok := <-sub1.ch:
		assert.False(t, ok, "Subscriber 1 channel should be closed after shutdown")
	default:
		t.Error("Subscriber 1 channel should be closed")
	}

	select {
	case _, ok := <-sub2.ch:
		assert.False(t, ok, "Subscriber 2 channel should be closed after shutdown")
	default:
		t.Error("Subscriber 2 channel should be closed")
	}

	// Verify dispatcher can be restarted
	dispatcher.started.Store(false) // Reset state for restart
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()

	candleCh2 := make(chan model.OHLCCandle, 10)
	defer close(candleCh2)

	err = dispatcher.StartDispatching(ctx2, candleCh2)
	assert.NoError(t, err, "Should be able to restart dispatcher after shutdown")
}

// Test_SubscriberStruct tests the Subscriber struct behavior
func Test_SubscriberStruct(t *testing.T) {
	symbols := []string{"BTC-USDT", "ETH-USDT", "SOL-USDT"}
	symbolsMap := make(map[string]struct{})
	for _, sym := range symbols {
		symbolsMap[sym] = struct{}{}
	}

	sub := &Subscriber{
		ch:                make(chan model.OHLCCandle, 100),
		symbolsSubscribed: symbolsMap,
	}

	// Test channel properties
	assert.Equal(t, 100, cap(sub.ch), "Should have correct channel capacity")
	assert.Equal(t, 0, len(sub.ch), "Should start with empty channel")

	// Test symbols map
	assert.Equal(t, len(symbols), len(sub.symbolsSubscribed), "Should have correct number of symbols")
	for _, sym := range symbols {
		_, exists := sub.symbolsSubscribed[sym]
		assert.True(t, exists, "Should contain symbol: %s", sym)
	}

	// Test channel can receive candles
	testCandle := createTestCandle("BTC-USDT", 50000)
	sub.ch <- testCandle

	assert.Equal(t, 1, len(sub.ch), "Should have one candle in channel")

	receivedCandle := <-sub.ch
	assert.Equal(t, testCandle.Pair, receivedCandle.Pair, "Should receive correct candle")
	assert.True(t, testCandle.Close.Equal(receivedCandle.Close), "Should receive correct price")
}

// Test_DispatcherConfig tests the configuration struct
func Test_DispatcherConfig(t *testing.T) {
	tests := []struct {
		name        string
		config      DispatcherConfig
		description string
	}{
		{
			name:        "Standard configuration",
			config:      DispatcherConfig{MaxSymbolsAllowed: 50},
			description: "Should handle standard configuration",
		},
		{
			name:        "Minimum configuration",
			config:      DispatcherConfig{MaxSymbolsAllowed: 1},
			description: "Should handle minimum configuration",
		},
		{
			name:        "Maximum configuration",
			config:      DispatcherConfig{MaxSymbolsAllowed: 10000},
			description: "Should handle large configuration",
		},
		{
			name:        "Zero configuration",
			config:      DispatcherConfig{MaxSymbolsAllowed: 0},
			description: "Should handle zero configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dispatcher := NewDispatcher(tt.config)
			assert.Equal(t, tt.config.MaxSymbolsAllowed, dispatcher.cfg.MaxSymbolsAllowed, tt.description)
		})
	}
}

// Test_EdgeCases tests various edge cases and error conditions
func Test_EdgeCases(t *testing.T) {

	t.Run("Multiple rapid subscribe/unsubscribe", func(t *testing.T) {
		dispatcher := NewDispatcher(createTestConfig())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		candleCh := make(chan model.OHLCCandle, 10)
		defer close(candleCh)

		err := dispatcher.StartDispatching(ctx, candleCh)
		require.NoError(t, err, "Should start dispatcher")

		// Give dispatcher time to start
		time.Sleep(10 * time.Millisecond)

		// Rapid subscribe/unsubscribe cycles
		for i := 0; i < 10; i++ {
			sub, err := dispatcher.Subscribe([]string{"BTC-USDT"})
			assert.NoError(t, err, "Should subscribe successfully in iteration %d", i)

			err = dispatcher.Unsubscribe(sub)
			assert.NoError(t, err, "Should unsubscribe successfully in iteration %d", i)

			// Small delay to allow processing
			time.Sleep(5 * time.Millisecond)
		}
	})

	t.Run("Send candle to closed channel scenario", func(t *testing.T) {
		dispatcher := NewDispatcher(createTestConfig())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		candleCh := make(chan model.OHLCCandle, 10)
		defer close(candleCh)

		err := dispatcher.StartDispatching(ctx, candleCh)
		require.NoError(t, err, "Should start dispatcher")

		// Give dispatcher time to start
		time.Sleep(10 * time.Millisecond)

		sub, err := dispatcher.Subscribe([]string{"BTC-USDT"})
		require.NoError(t, err, "Should create subscriber")

		// Give time for subscription
		time.Sleep(10 * time.Millisecond)

		// Unsubscribe (closes channel)
		err = dispatcher.Unsubscribe(sub)
		require.NoError(t, err, "Should unsubscribe")

		// Give time for unsubscription
		time.Sleep(10 * time.Millisecond)

		// Send candle - should not panic even though subscriber is gone
		candle := createTestCandle("BTC-USDT", 50000)
		candleCh <- candle

		// Give time for processing
		time.Sleep(10 * time.Millisecond)

		// Test should complete without panic
	})
}

// Benchmark_MessageDistribution benchmarks the distribution performance
func Benchmark_MessageDistribution(b *testing.B) {
	dispatcher := NewDispatcher(DispatcherConfig{MaxSymbolsAllowed: 100})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleCh := make(chan model.OHLCCandle, 1000)
	defer close(candleCh)

	err := dispatcher.StartDispatching(ctx, candleCh)
	if err != nil {
		b.Fatal(err)
	}

	// Give dispatcher time to start
	time.Sleep(10 * time.Millisecond)

	// Create multiple subscribers
	numSubscribers := 100
	subscribers := make([]*Subscriber, numSubscribers)

	for i := 0; i < numSubscribers; i++ {
		sub, err := dispatcher.Subscribe([]string{"BTC-USDT"})
		if err != nil {
			b.Fatal(err)
		}
		subscribers[i] = sub

		// Consume messages to prevent blocking
		go func(s *Subscriber) {
			for range s.ch {
				// Discard messages
			}
		}(sub)
	}

	// Give time for subscriptions
	time.Sleep(50 * time.Millisecond)

	candle := createTestCandle("BTC-USDT", 50000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		candleCh <- candle
	}
}

// Benchmark_Subscribe benchmarks subscription creation performance
func Benchmark_Subscribe(b *testing.B) {
	dispatcher := NewDispatcher(DispatcherConfig{MaxSymbolsAllowed: 100})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candleCh := make(chan model.OHLCCandle, 1000)
	defer close(candleCh)

	err := dispatcher.StartDispatching(ctx, candleCh)
	if err != nil {
		b.Fatal(err)
	}

	// Give dispatcher time to start
	time.Sleep(10 * time.Millisecond)

	pairs := []string{"BTC-USDT", "ETH-USDT", "SOL-USDT"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sub, err := dispatcher.Subscribe(pairs)
		if err != nil {
			b.Fatal(err)
		}

		// Clean up to prevent memory issues
		if i%100 == 0 {
			err = dispatcher.Unsubscribe(sub)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}
