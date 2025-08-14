package service

import (
	pb "candles/gen/proto"
	"candles/internal/model"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// Mock implementations
type MockTradeCandleAggregator struct {
	mock.Mock
	candleChannel chan model.OHLCCandle
	started       atomic.Bool
	closed        atomic.Bool
}

func NewMockTradeCandleAggregator() *MockTradeCandleAggregator {
	return &MockTradeCandleAggregator{
		candleChannel: make(chan model.OHLCCandle, 100),
	}
}

func (m *MockTradeCandleAggregator) StartCandleStream(ctx context.Context, pairs []string) (<-chan model.OHLCCandle, error) {
	args := m.Called(ctx, pairs)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	m.started.Store(true)

	// Start a goroutine to close channel when context is cancelled
	go func() {
		<-ctx.Done()
		if !m.closed.Load() {
			m.closed.Store(true)
			close(m.candleChannel)
		}
	}()

	return m.candleChannel, nil
}

func (m *MockTradeCandleAggregator) SendCandle(candle model.OHLCCandle) {
	if m.started.Load() && !m.closed.Load() {
		select {
		case m.candleChannel <- candle:
		default:
			// Channel full, skip
		}
	}
}

func (m *MockTradeCandleAggregator) Close() {
	if !m.closed.Load() {
		m.closed.Store(true)
		close(m.candleChannel)
	}
}

type MockSubscriptionManager struct {
	mock.Mock
	subscribers    map[int64]*Subscriber
	mu             sync.RWMutex
	dispatchChan   <-chan model.OHLCCandle
	dispatchCtx    context.Context
	dispatchCancel context.CancelFunc
	started        atomic.Bool
}

func NewMockSubscriptionManager() *MockSubscriptionManager {
	return &MockSubscriptionManager{
		subscribers: make(map[int64]*Subscriber),
	}
}

func (m *MockSubscriptionManager) Subscribe(pairs []string) (*Subscriber, error) {
	args := m.Called(pairs)

	if args.Error(1) != nil {
		return nil, args.Error(1)
	}

	// If a mock subscriber was provided, return it
	if args.Get(0) != nil {
		sub := args.Get(0).(*Subscriber)
		m.mu.Lock()
		m.subscribers[sub.id] = sub
		m.mu.Unlock()
		return sub, nil
	}

	// Create a new subscriber if none was provided in the mock
	sub := &Subscriber{
		id:                int64(len(m.subscribers) + 1), // Simple ID generation
		ch:                make(chan model.OHLCCandle, 100),
		symbolsSubscribed: make(map[string]struct{}),
	}

	for _, pair := range pairs {
		sub.symbolsSubscribed[pair] = struct{}{}
	}

	m.mu.Lock()
	m.subscribers[sub.id] = sub
	m.mu.Unlock()

	return sub, nil
}

func (m *MockSubscriptionManager) Unsubscribe(sub *Subscriber) error {
	args := m.Called(sub)

	if args.Error(0) != nil {
		return args.Error(0)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.subscribers[sub.id]; !exists {
		return errors.New("subscriber not found")
	}

	delete(m.subscribers, sub.id)

	// Safely close the channel if it's not already closed
	select {
	case <-sub.ch:
		// Channel is already closed
	default:
		close(sub.ch)
	}

	return nil
}

func (m *MockSubscriptionManager) StartDispatching(ctx context.Context, ch <-chan model.OHLCCandle) error {
	args := m.Called(ctx, ch)

	if args.Error(0) != nil {
		return args.Error(0)
	}

	m.dispatchCtx, m.dispatchCancel = context.WithCancel(ctx)
	m.dispatchChan = ch
	m.started.Store(true)

	// Start dispatching in background
	go m.dispatch()

	return nil
}

func (m *MockSubscriptionManager) dispatch() {
	defer m.dispatchCancel()

	for {
		select {
		case <-m.dispatchCtx.Done():
			return
		case candle, ok := <-m.dispatchChan:
			if !ok {
				return
			}
			m.distributeCandle(candle)
		}
	}
}

func (m *MockSubscriptionManager) distributeCandle(candle model.OHLCCandle) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, sub := range m.subscribers {
		if _, ok := sub.symbolsSubscribed[candle.Pair]; ok {
			select {
			case sub.ch <- candle:
			default:
				// Channel full, skip
			}
		}
	}
}

func (m *MockSubscriptionManager) GetSubscriberCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subscribers)
}

// Mock gRPC stream
type MockSubscribeServer struct {
	mock.Mock
	ctx         context.Context
	sentCandles []pb.Candle
	mu          sync.Mutex
	sendError   error
}

func (m *MockSubscribeServer) SetHeader(md metadata.MD) error {
	return nil
}

func (m *MockSubscribeServer) SendHeader(md metadata.MD) error {
	return nil
}

func (m *MockSubscribeServer) SetTrailer(md metadata.MD) {
	// No-op for mock
}

func (m *MockSubscribeServer) SendMsg(arg any) error {
	return nil
}

func (m *MockSubscribeServer) RecvMsg(arg any) error {
	return nil
}

func NewMockSubscribeServer(ctx context.Context) *MockSubscribeServer {
	return &MockSubscribeServer{
		ctx:         ctx,
		sentCandles: make([]pb.Candle, 0),
	}
}

func (m *MockSubscribeServer) Send(candle *pb.Candle) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.sendError != nil {
		return m.sendError
	}

	m.sentCandles = append(m.sentCandles, *candle)
	return nil
}

func (m *MockSubscribeServer) Context() context.Context {
	return m.ctx
}

func (m *MockSubscribeServer) GetSentCandles() []pb.Candle {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]pb.Candle, len(m.sentCandles))
	copy(result, m.sentCandles)
	return result
}

func (m *MockSubscribeServer) SetSendError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sendError = err
}

// Test helper functions
func createCandleServiceTestCandle(pair string) model.OHLCCandle {
	now := time.Now()
	return model.OHLCCandle{
		Pair:      pair,
		Open:      decimal.NewFromFloat(100.0),
		High:      decimal.NewFromFloat(105.0),
		Low:       decimal.NewFromFloat(95.0),
		Close:     decimal.NewFromFloat(102.0),
		Volume:    decimal.NewFromFloat(1000.0),
		StartTime: now.Add(-time.Minute),
		EndTime:   now,
	}
}

// Test NewCandleService
func TestNewCandleService(t *testing.T) {
	t.Run("Creates service with valid dependencies", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		assert.NotNil(t, service, "Service should not be nil")
		assert.Equal(t, manager, service.subscriptionManager, "Should store subscription manager")
		assert.Equal(t, aggregator, service.candleAggregator, "Should store candle aggregator")
		assert.False(t, service.started.Load(), "Should start in stopped state")
		assert.Nil(t, service.cancel, "Cancel function should be nil initially")
	})

	t.Run("Handles nil dependencies gracefully", func(t *testing.T) {
		// Test with nil manager
		service1 := NewCandleService(nil, NewMockTradeCandleAggregator())
		assert.NotNil(t, service1, "Should create service even with nil manager")

		// Test with nil aggregator
		service2 := NewCandleService(NewMockSubscriptionManager(), nil)
		assert.NotNil(t, service2, "Should create service even with nil aggregator")

		// Test with both nil
		service3 := NewCandleService(nil, nil)
		assert.NotNil(t, service3, "Should create service even with both nil")
	})
}

// Test Start method
func TestCandleService_Start(t *testing.T) {
	t.Run("Successful start", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT", "ETH-USDT"}

		// Setup mock expectations
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		service := NewCandleService(manager, aggregator)

		ctx := context.Background()
		err := service.Start(ctx, pairs)

		assert.NoError(t, err, "Start should succeed")
		assert.True(t, service.started.Load(), "Service should be marked as started")
		assert.NotNil(t, service.cancel, "Cancel function should be set")

		aggregator.AssertExpectations(t)
		manager.AssertExpectations(t)
	})

	t.Run("Fails when already started", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT"}

		// Setup mock expectations for first start
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		service := NewCandleService(manager, aggregator)

		ctx := context.Background()

		// First start should succeed
		err := service.Start(ctx, pairs)
		assert.NoError(t, err, "First start should succeed")

		// Second start should fail
		err = service.Start(ctx, pairs)
		assert.Error(t, err, "Second start should fail")
		assert.Contains(t, err.Error(), "already started", "Error should mention already started")
	})

	t.Run("Fails when aggregator fails to start", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT"}
		aggregatorError := errors.New("aggregator startup failed")

		// Setup mock expectations
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(nil, aggregatorError)

		service := NewCandleService(manager, aggregator)

		ctx := context.Background()
		err := service.Start(ctx, pairs)

		assert.Error(t, err, "Start should fail when aggregator fails")
		assert.Contains(t, err.Error(), "failed to start aggregator", "Error should mention aggregator failure")
		assert.False(t, service.started.Load(), "Service should not be marked as started")
		assert.Nil(t, service.cancel, "Cancel function should remain nil")

		aggregator.AssertExpectations(t)
	})

	t.Run("Fails when dispatcher fails to start", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT"}
		dispatcherError := errors.New("dispatcher startup failed")

		// Setup mock expectations
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(dispatcherError)

		service := NewCandleService(manager, aggregator)

		ctx := context.Background()
		err := service.Start(ctx, pairs)

		assert.Error(t, err, "Start should fail when dispatcher fails")
		assert.Contains(t, err.Error(), "failed to start dispatching", "Error should mention dispatching failure")
		assert.False(t, service.started.Load(), "Service should not be marked as started")
		assert.Nil(t, service.cancel, "Cancel function should remain nil")

		aggregator.AssertExpectations(t)
		manager.AssertExpectations(t)
	})

	t.Run("Empty pairs list", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{}

		// Setup mock expectations
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		service := NewCandleService(manager, aggregator)

		ctx := context.Background()
		err := service.Start(ctx, pairs)

		assert.NoError(t, err, "Start should succeed with empty pairs")
		assert.True(t, service.started.Load(), "Service should be marked as started")
	})

	t.Run("Context with timeout", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT"}

		// Setup mock expectations
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		service := NewCandleService(manager, aggregator)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := service.Start(ctx, pairs)

		assert.NoError(t, err, "Start should succeed with timeout context")
		assert.True(t, service.started.Load(), "Service should be marked as started")
	})
}

// Test Stop method
func TestCandleService_Stop(t *testing.T) {
	t.Run("Successful stop", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT"}

		// Setup mock expectations
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		service := NewCandleService(manager, aggregator)

		// Start service first
		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Start should succeed")

		// Stop service
		err = service.Stop()

		assert.NoError(t, err, "Stop should succeed")
		assert.False(t, service.started.Load(), "Service should be marked as stopped")
		assert.Nil(t, service.cancel, "Cancel function should be nil after stop")
	})

	t.Run("Fails when not started", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		err := service.Stop()

		assert.Error(t, err, "Stop should fail when not started")
		assert.Contains(t, err.Error(), "service not started", "Error should mention service not started")
		assert.False(t, service.started.Load(), "Service should remain stopped")
	})

	t.Run("Multiple stop calls", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT"}

		// Setup mock expectations
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		service := NewCandleService(manager, aggregator)

		// Start service
		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Start should succeed")

		// First stop should succeed
		err = service.Stop()
		assert.NoError(t, err, "First stop should succeed")

		// Second stop should fail
		err = service.Stop()
		assert.Error(t, err, "Second stop should fail")
		assert.Contains(t, err.Error(), "service not started", "Error should mention service not started")
	})

	t.Run("Stop without cancel function", func(t *testing.T) {
		service := &CandleService{}
		service.started.Store(true)

		err := service.Stop()

		assert.NoError(t, err, "Stop should succeed even without cancel function")
		assert.False(t, service.started.Load(), "Service should be marked as stopped")
	})
}

// Test Subscribe method
func TestCandleService_Subscribe(t *testing.T) {
	t.Run("Successful subscription", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT", "ETH-USDT"}

		// Setup service
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		service := NewCandleService(manager, aggregator)

		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		// Setup subscription
		mockSub := &Subscriber{
			id:                0,
			ch:                make(chan model.OHLCCandle, 100),
			symbolsSubscribed: map[string]struct{}{"BTC-USDT": struct{}{}, "ETH-USDT": struct{}{}},
		}

		manager.On("Subscribe", pairs).Return(mockSub, nil)
		manager.On("Unsubscribe", mockSub).Return(nil)

		// Create mock stream
		streamCtx, streamCancel := context.WithCancel(context.Background())
		mockStream := NewMockSubscribeServer(streamCtx)

		// Create subscription request
		req := &pb.SubscriptionRequest{
			Symbols: pairs,
		}

		// Start subscription in goroutine and cancel after sending some data
		go func() {
			time.Sleep(100 * time.Millisecond)

			// Send test candle
			testCandle := createCandleServiceTestCandle("BTC-USDT")
			mockSub.ch <- testCandle

			time.Sleep(100 * time.Millisecond)
			streamCancel() // Cancel to end subscription
		}()

		err = service.Subscribe(req, mockStream)

		assert.NoError(t, err, "Subscribe should succeed")

		// Verify candle was sent
		sentCandles := mockStream.GetSentCandles()
		assert.Len(t, sentCandles, 1, "Should send one candle")
		assert.Equal(t, "BTC-USDT", sentCandles[0].Symbol, "Should send correct symbol")

		manager.AssertExpectations(t)
	})

	t.Run("Fails when service not started", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		req := &pb.SubscriptionRequest{
			Symbols: []string{"BTC-USDT"},
		}

		mockStream := NewMockSubscribeServer(context.Background())

		err := service.Subscribe(req, mockStream)

		assert.Error(t, err, "Subscribe should fail when service not started")
		assert.Contains(t, err.Error(), "candle service not started", "Error should mention service not started")
	})

	t.Run("Fails with nil request", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		// Start service
		pairs := []string{"BTC-USDT"}
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		mockStream := NewMockSubscribeServer(context.Background())

		err = service.Subscribe(nil, mockStream)

		assert.Error(t, err, "Subscribe should fail with nil request")
		assert.Contains(t, err.Error(), "request cannot be nil", "Error should mention nil request")
	})

	t.Run("Fails with empty symbols", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		// Start service
		pairs := []string{"BTC-USDT"}
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		req := &pb.SubscriptionRequest{
			Symbols: []string{},
		}

		mockStream := NewMockSubscribeServer(context.Background())

		err = service.Subscribe(req, mockStream)

		assert.Error(t, err, "Subscribe should fail with empty symbols")
		assert.Contains(t, err.Error(), "no symbols provided", "Error should mention no symbols")
	})

	t.Run("Fails with invalid symbol", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		// Start service
		pairs := []string{"BTC-USDT"}
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		req := &pb.SubscriptionRequest{
			Symbols: []string{"INVALID_SYMBOL", "BTC-USDT"},
		}

		mockStream := NewMockSubscribeServer(context.Background())

		err = service.Subscribe(req, mockStream)

		assert.Error(t, err, "Subscribe should fail with invalid symbol")
		assert.Contains(t, err.Error(), "invalid symbol at index 0", "Error should mention invalid symbol")
	})

	t.Run("Fails when subscription manager fails", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		// Start service
		pairs := []string{"BTC-USDT"}
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		// Setup subscription failure
		subscriptionError := errors.New("subscription failed")
		manager.On("Subscribe", pairs).Return(nil, subscriptionError)

		req := &pb.SubscriptionRequest{
			Symbols: pairs,
		}

		mockStream := NewMockSubscribeServer(context.Background())

		err = service.Subscribe(req, mockStream)

		assert.Error(t, err, "Subscribe should fail when subscription manager fails")
		assert.Contains(t, err.Error(), "failed to subscribe", "Error should mention subscription failure")

		manager.AssertExpectations(t)
	})

	t.Run("Handles stream send error", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		// Start service
		pairs := []string{"BTC-USDT"}
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		// Setup subscription
		mockSub := &Subscriber{
			ch:                make(chan model.OHLCCandle, 100),
			symbolsSubscribed: map[string]struct{}{"BTC-USDT": struct{}{}},
		}

		manager.On("Subscribe", pairs).Return(mockSub, nil)
		manager.On("Unsubscribe", mockSub).Return(nil)

		// Create mock stream with send error
		mockStream := NewMockSubscribeServer(context.Background())
		sendError := errors.New("stream send failed")
		mockStream.SetSendError(sendError)

		req := &pb.SubscriptionRequest{
			Symbols: pairs,
		}

		// Send candle to trigger send error
		go func() {
			time.Sleep(50 * time.Millisecond)
			testCandle := createCandleServiceTestCandle("BTC-USDT")
			mockSub.ch <- testCandle
		}()

		err = service.Subscribe(req, mockStream)

		assert.Error(t, err, "Subscribe should fail when stream send fails")
		assert.Contains(t, err.Error(), "failed to send candle", "Error should mention send failure")

		manager.AssertExpectations(t)
	})

	t.Run("Handles subscription channel close", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		// Start service
		pairs := []string{"BTC-USDT"}
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		// Setup subscription
		mockSub := &Subscriber{
			ch:                make(chan model.OHLCCandle, 100),
			symbolsSubscribed: map[string]struct{}{"BTC-USDT": struct{}{}},
		}

		manager.On("Subscribe", pairs).Return(mockSub, nil)
		manager.On("Unsubscribe", mockSub).Return(nil)

		mockStream := NewMockSubscribeServer(context.Background())

		req := &pb.SubscriptionRequest{
			Symbols: pairs,
		}

		// Close subscription channel to trigger exit
		go func() {
			time.Sleep(50 * time.Millisecond)
			close(mockSub.ch)
		}()

		err = service.Subscribe(req, mockStream)

		assert.NoError(t, err, "Subscribe should succeed when channel closes")

		manager.AssertExpectations(t)
	})

	t.Run("Multiple symbols validation", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		service := NewCandleService(manager, aggregator)

		// Start service
		pairs := []string{"BTC-USDT"}
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(make(<-chan model.OHLCCandle), nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		req := &pb.SubscriptionRequest{
			Symbols: []string{"BTC-USDT", "INVALID", "ETH-USDT"},
		}

		mockStream := NewMockSubscribeServer(context.Background())

		err = service.Subscribe(req, mockStream)

		assert.Error(t, err, "Subscribe should fail with invalid symbol in list")
		assert.Contains(t, err.Error(), "invalid symbol at index 1", "Error should mention correct index")
		assert.Contains(t, err.Error(), "INVALID", "Error should mention invalid symbol")
	})
}

// Test toCandle helper function
func TestToCandle(t *testing.T) {
	t.Run("Converts OHLC candle correctly", func(t *testing.T) {
		ohlc := createCandleServiceTestCandle("BTC-USDT")

		pb := toCandle(ohlc)

		assert.Equal(t, "BTC-USDT", pb.Symbol, "Should convert symbol correctly")
		assert.Equal(t, "100", pb.Open, "Should convert open price correctly")
		assert.Equal(t, "105", pb.High, "Should convert high price correctly")
		assert.Equal(t, "95", pb.Low, "Should convert low price correctly")
		assert.Equal(t, "102", pb.Close, "Should convert close price correctly")
		assert.Equal(t, "1000", pb.Volume, "Should convert volume correctly")

		// Test timestamps
		expectedStart := fmt.Sprintf("%d", ohlc.StartTime.UnixMilli())
		expectedEnd := fmt.Sprintf("%d", ohlc.EndTime.UnixMilli())
		assert.Equal(t, expectedStart, pb.StartTimestamp, "Should convert start timestamp correctly")
		assert.Equal(t, expectedEnd, pb.EndTimestamp, "Should convert end timestamp correctly")
	})

	t.Run("Handles zero values", func(t *testing.T) {
		ohlc := model.OHLCCandle{
			Pair:      "TEST-PAIR",
			Open:      decimal.Zero,
			High:      decimal.Zero,
			Low:       decimal.Zero,
			Close:     decimal.Zero,
			Volume:    decimal.Zero,
			StartTime: time.Unix(0, 0),
			EndTime:   time.Unix(0, 0),
		}

		pb := toCandle(ohlc)

		assert.Equal(t, "TEST-PAIR", pb.Symbol, "Should handle symbol correctly")
		assert.Equal(t, "0", pb.Open, "Should handle zero open price")
		assert.Equal(t, "0", pb.High, "Should handle zero high price")
		assert.Equal(t, "0", pb.Low, "Should handle zero low price")
		assert.Equal(t, "0", pb.Close, "Should handle zero close price")
		assert.Equal(t, "0", pb.Volume, "Should handle zero volume")
		assert.Equal(t, "0", pb.StartTimestamp, "Should handle zero start timestamp")
		assert.Equal(t, "0", pb.EndTimestamp, "Should handle zero end timestamp")
	})

	t.Run("Handles negative values", func(t *testing.T) {
		ohlc := model.OHLCCandle{
			Pair:      "TEST-PAIR",
			Open:      decimal.NewFromFloat(-100.5),
			High:      decimal.NewFromFloat(-50.25),
			Low:       decimal.NewFromFloat(-150.75),
			Close:     decimal.NewFromFloat(-75.125),
			Volume:    decimal.NewFromFloat(-10.5),
			StartTime: time.Unix(0, 0),
			EndTime:   time.Unix(0, 0),
		}

		pb := toCandle(ohlc)

		assert.Equal(t, "-100.5", pb.Open, "Should handle negative open price")
		assert.Equal(t, "-50.25", pb.High, "Should handle negative high price")
		assert.Equal(t, "-150.75", pb.Low, "Should handle negative low price")
		assert.Equal(t, "-75.125", pb.Close, "Should handle negative close price")
		assert.Equal(t, "-10.5", pb.Volume, "Should handle negative volume")
	})

	t.Run("Handles very large values", func(t *testing.T) {
		ohlc := model.OHLCCandle{
			Pair:      "TEST-PAIR",
			Open:      decimal.NewFromFloat(999999999.999999),
			High:      decimal.NewFromFloat(1000000000.0),
			Low:       decimal.NewFromFloat(999999999.0),
			Close:     decimal.NewFromFloat(999999999.5),
			Volume:    decimal.NewFromFloat(1000000000000.0),
			StartTime: time.Unix(1<<31-1, 0), // Max int32
			EndTime:   time.Unix(1<<31-1, 999999999),
		}

		pb := toCandle(ohlc)

		assert.Contains(t, pb.Open, "999999999", "Should handle large open price")
		assert.Contains(t, pb.High, "1000000000", "Should handle large high price")
		assert.Contains(t, pb.Volume, "1000000000000", "Should handle large volume")
	})

	t.Run("Handles precision correctly", func(t *testing.T) {
		ohlc := model.OHLCCandle{
			Pair:   "TEST-PAIR",
			Open:   decimal.NewFromFloat(123.456789),
			High:   decimal.NewFromFloat(123.456790),
			Low:    decimal.NewFromFloat(123.456788),
			Close:  decimal.NewFromFloat(123.456789),
			Volume: decimal.NewFromFloat(0.000001),
		}

		pb := toCandle(ohlc)

		assert.Equal(t, "123.456789", pb.Open, "Should preserve precision for open")
		assert.Equal(t, "123.45679", pb.High, "Should preserve precision for high")
		assert.Equal(t, "123.456788", pb.Low, "Should preserve precision for low")
		assert.Equal(t, "123.456789", pb.Close, "Should preserve precision for close")
		assert.Equal(t, "0.000001", pb.Volume, "Should preserve precision for volume")
	})
}

// Integration tests
func TestCandleService_Integration(t *testing.T) {
	t.Run("Full lifecycle test", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT", "ETH-USDT"}

		// Setup mock expectations
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(aggregator.candleChannel, nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		service := NewCandleService(manager, aggregator)

		// Start service
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		// Setup subscription
		mockSub := &Subscriber{
			ch:                make(chan model.OHLCCandle, 100),
			symbolsSubscribed: map[string]struct{}{"BTC-USDT": struct{}{}, "ETH-USDT": struct{}{}},
		}

		manager.On("Subscribe", pairs).Return(mockSub, nil)
		manager.On("Unsubscribe", mockSub).Return(nil)

		// Create subscription
		req := &pb.SubscriptionRequest{
			Symbols: pairs,
		}

		streamCtx, streamCancel := context.WithCancel(context.Background())
		mockStream := NewMockSubscribeServer(streamCtx)

		// Test end-to-end flow
		var subscriptionErr error
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			subscriptionErr = service.Subscribe(req, mockStream)
		}()

		// Send test candles
		time.Sleep(50 * time.Millisecond)

		testCandles := []model.OHLCCandle{
			createCandleServiceTestCandle("BTC-USDT"),
			createCandleServiceTestCandle("ETH-USDT"),
			createCandleServiceTestCandle("BTC-USDT"),
		}

		for _, candle := range testCandles {
			mockSub.ch <- candle
			time.Sleep(10 * time.Millisecond)
		}

		time.Sleep(100 * time.Millisecond)
		streamCancel()

		wg.Wait()

		assert.NoError(t, subscriptionErr, "Subscription should succeed")

		// Verify candles were sent
		sentCandles := mockStream.GetSentCandles()
		assert.Len(t, sentCandles, 3, "Should send all test candles")

		// Stop service
		err = service.Stop()
		assert.NoError(t, err, "Service should stop successfully")

		manager.AssertExpectations(t)
		aggregator.AssertExpectations(t)
	})

	t.Run("Concurrent subscriptions", func(t *testing.T) {
		manager := NewMockSubscriptionManager()
		aggregator := NewMockTradeCandleAggregator()

		pairs := []string{"BTC-USDT"}

		// Setup mock expectations
		aggregator.On("StartCandleStream", mock.Anything, pairs).Return(aggregator.candleChannel, nil)
		manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

		service := NewCandleService(manager, aggregator)

		// Start service
		ctx := context.Background()
		err := service.Start(ctx, pairs)
		require.NoError(t, err, "Service should start")

		// Create multiple subscriptions concurrently
		numSubscriptions := 5
		var wg sync.WaitGroup

		for i := 0; i < numSubscriptions; i++ {
			wg.Add(1)

			go func(id int) {
				defer wg.Done()

				// Setup subscription
				mockSub := &Subscriber{
					ch:                make(chan model.OHLCCandle, 100),
					symbolsSubscribed: map[string]struct{}{"BTC-USDT": struct{}{}},
				}

				manager.On("Subscribe", pairs).Return(mockSub, nil)
				manager.On("Unsubscribe", mockSub).Return(nil)

				streamCtx, streamCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
				defer streamCancel()

				mockStream := NewMockSubscribeServer(streamCtx)

				req := &pb.SubscriptionRequest{
					Symbols: pairs,
				}

				err := service.Subscribe(req, mockStream)
				// Should succeed or timeout (both acceptable for this test)
				if err != nil {
					t.Logf("Subscription %d error: %v", id, err)
				}
			}(i)
		}

		wg.Wait()

		// Stop service
		err = service.Stop()
		assert.NoError(t, err, "Service should stop")
	})
}

// Benchmark tests
func BenchmarkCandleService_Subscribe(b *testing.B) {
	manager := NewMockSubscriptionManager()
	aggregator := NewMockTradeCandleAggregator()

	pairs := []string{"BTC-USDT"}

	aggregator.On("StartCandleStream", mock.Anything, pairs).Return(aggregator.candleChannel, nil)
	manager.On("StartDispatching", mock.Anything, mock.Anything).Return(nil)

	service := NewCandleService(manager, aggregator)

	ctx := context.Background()
	err := service.Start(ctx, pairs)
	if err != nil {
		b.Fatal(err)
	}
	defer service.Stop()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		mockSub := &Subscriber{
			ch:                make(chan model.OHLCCandle, 1),
			symbolsSubscribed: map[string]struct{}{"BTC-USDT": struct{}{}},
		}

		manager.On("Subscribe", pairs).Return(mockSub, nil).Once()
		manager.On("Unsubscribe", mockSub).Return(nil).Once()

		streamCtx, streamCancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		mockStream := NewMockSubscribeServer(streamCtx)

		req := &pb.SubscriptionRequest{
			Symbols: pairs,
		}

		service.Subscribe(req, mockStream)
		streamCancel()
	}
}

func BenchmarkToCandle(b *testing.B) {
	ohlc := createCandleServiceTestCandle("BTC-USDT")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = toCandle(ohlc)
	}
}
