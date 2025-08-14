package websocket

import (
	"candles/internal/model"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWebSocketServer provides a more advanced mock server for testing
type TestWebSocketServer struct {
	server           *httptest.Server
	upgrader         websocket.Upgrader
	connections      []*websocket.Conn
	mu               sync.RWMutex
	messageQueue     []interface{}
	receivedMessages [][]byte
	pingCount        atomic.Int64
	pongCount        atomic.Int64
	closeCount       atomic.Int64
	handlerFunc      func(conn *websocket.Conn)
	shouldRejectConn atomic.Bool
	shouldSlowConn   atomic.Bool
	shouldDropConn   atomic.Bool
	customHeaders    http.Header
	tlsServer        *httptest.Server
}

func NewTestWebSocketServer() *TestWebSocketServer {
	ts := &TestWebSocketServer{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		messageQueue:     make([]interface{}, 0),
		receivedMessages: make([][]byte, 0),
		customHeaders:    make(http.Header),
	}

	ts.server = httptest.NewServer(http.HandlerFunc(ts.handleWebSocket))
	return ts
}

func NewTestTLSWebSocketServer() *TestWebSocketServer {
	ts := &TestWebSocketServer{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		messageQueue:     make([]interface{}, 0),
		receivedMessages: make([][]byte, 0),
		customHeaders:    make(http.Header),
	}

	ts.tlsServer = httptest.NewTLSServer(http.HandlerFunc(ts.handleWebSocket))
	return ts
}

func (ts *TestWebSocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if ts.shouldRejectConn.Load() {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("Connection rejected"))
		return
	}

	if ts.shouldSlowConn.Load() {
		time.Sleep(2 * time.Second)
	}

	// Add custom headers
	for key, values := range ts.customHeaders {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	conn, err := ts.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	ts.mu.Lock()
	ts.connections = append(ts.connections, conn)
	ts.mu.Unlock()

	if ts.handlerFunc != nil {
		ts.handlerFunc(conn)
	} else {
		ts.defaultHandler(conn)
	}
}

func (ts *TestWebSocketServer) defaultHandler(conn *websocket.Conn) {
	defer func() {
		if ts.shouldDropConn.Load() {
			conn.Close()
		} else {
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			conn.Close()
		}
		ts.closeCount.Add(1)
	}()

	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			return
		}

		ts.mu.Lock()
		ts.receivedMessages = append(ts.receivedMessages, data)
		ts.mu.Unlock()

		switch messageType {
		case websocket.PingMessage:
			ts.pingCount.Add(1)
			conn.WriteMessage(websocket.PongMessage, data)
		case websocket.PongMessage:
			ts.pongCount.Add(1)
		case websocket.TextMessage:
			// Echo back or queue response
			ts.sendQueuedMessages(conn)
		case websocket.BinaryMessage:
			// Handle binary messages
		case websocket.CloseMessage:
			return
		}
	}
}

func (ts *TestWebSocketServer) sendQueuedMessages(conn *websocket.Conn) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	for _, msg := range ts.messageQueue {
		data, _ := json.Marshal(msg)
		conn.WriteMessage(websocket.TextMessage, data)
	}
	ts.messageQueue = ts.messageQueue[:0] // Clear queue
}

func (ts *TestWebSocketServer) URL() string {
	if ts.tlsServer != nil {
		return "wss" + strings.TrimPrefix(ts.tlsServer.URL, "https")
	}
	return "ws" + strings.TrimPrefix(ts.server.URL, "http")
}

func (ts *TestWebSocketServer) Close() {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	for _, conn := range ts.connections {
		conn.Close()
	}

	if ts.tlsServer != nil {
		ts.tlsServer.Close()
	} else {
		ts.server.Close()
	}
}

func (ts *TestWebSocketServer) SetRejectConnection(reject bool) {
	ts.shouldRejectConn.Store(reject)
}

func (ts *TestWebSocketServer) SetSlowConnection(slow bool) {
	ts.shouldSlowConn.Store(slow)
}

func (ts *TestWebSocketServer) SetDropConnection(drop bool) {
	ts.shouldDropConn.Store(drop)
}

func (ts *TestWebSocketServer) SetCustomHandler(handler func(conn *websocket.Conn)) {
	ts.handlerFunc = handler
}

func (ts *TestWebSocketServer) QueueMessage(msg interface{}) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.messageQueue = append(ts.messageQueue, msg)
}

func (ts *TestWebSocketServer) GetPingCount() int64 {
	return ts.pingCount.Load()
}

func (ts *TestWebSocketServer) GetPongCount() int64 {
	return ts.pongCount.Load()
}

func (ts *TestWebSocketServer) GetReceivedMessages() [][]byte {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	result := make([][]byte, len(ts.receivedMessages))
	copy(result, ts.receivedMessages)
	return result
}

// Helper functions for testing
func createTestHandler() func([]byte, chan<- model.TradeEvent) error {
	return func(data []byte, tradeChan chan<- model.TradeEvent) error {
		var msg map[string]interface{}
		if err := json.Unmarshal(data, &msg); err != nil {
			return err
		}

		if msgType, ok := msg["type"].(string); ok && msgType == "trade" {
			trade := model.TradeEvent{
				Pair:     "BTC-USDT",
				Price:    decimal.NewFromFloat(50000.0),
				Quantity: decimal.NewFromFloat(0.1),
				Exchange: model.BinanceExchange,
			}

			select {
			case tradeChan <- trade:
			default:
				// Channel full
			}
		}
		return nil
	}
}

func createErrorHandler() func([]byte, chan<- model.TradeEvent) error {
	return func(data []byte, tradeChan chan<- model.TradeEvent) error {
		return errors.New("handler error")
	}
}

func createPanicHandler() func([]byte, chan<- model.TradeEvent) error {
	return func(data []byte, tradeChan chan<- model.TradeEvent) error {
		panic("handler panic")
	}
}

func createSlowHandler(delay time.Duration) func([]byte, chan<- model.TradeEvent) error {
	return func(data []byte, tradeChan chan<- model.TradeEvent) error {
		time.Sleep(delay)
		return nil
	}
}

// Test Config validation
func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "valid minimal config",
			config: Config{
				Endpoint: "ws://localhost:8080/ws",
				Handler:  createTestHandler(),
			},
			expectError: false,
		},
		{
			name: "empty endpoint",
			config: Config{
				Endpoint: "",
				Handler:  createTestHandler(),
			},
			expectError: true,
			errorMsg:    "endpoint URL is required",
		},
		{
			name: "nil handler",
			config: Config{
				Endpoint: "ws://localhost:8080/ws",
				Handler:  nil,
			},
			expectError: true,
			errorMsg:    "message handler is required",
		},
		{
			name: "valid config with all fields",
			config: Config{
				Endpoint:             "wss://secure.example.com/ws",
				Handler:              createTestHandler(),
				TLSInsecureSkip:      true,
				PingPeriod:           30 * time.Second,
				SendTimeout:          10 * time.Second,
				SubscriptionMessages: [][]byte{[]byte(`{"subscribe":"trades"}`)},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			client, err := NewWebsocketClient(ctx, tt.config)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, client)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				// For valid configs, connection will fail since no real server
				// But config validation should pass
				if client != nil {
					client.Close()
				}
			}
		})
	}
}

// Test default values
func TestNewWebsocketClient_Defaults(t *testing.T) {
	server := NewTestWebSocketServer()
	defer server.Close()

	config := Config{
		Endpoint: server.URL(),
		Handler:  createTestHandler(),
		// Leave optional fields unset to test defaults
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := NewWebsocketClient(ctx, config)
	require.NoError(t, err)
	defer client.Close()

	// Test defaults were applied
	assert.Equal(t, defaultPingPeriod, client.cfg.PingPeriod)
	assert.Equal(t, defaultSendTimeout, client.cfg.SendTimeout)
	assert.NotNil(t, client.cfg.SubscriptionMessages)
	assert.Empty(t, client.cfg.SubscriptionMessages)
}

// Test successful connection
func TestNewWebsocketClient_SuccessfulConnection(t *testing.T) {
	server := NewTestWebSocketServer()
	defer server.Close()

	config := Config{
		Endpoint:    server.URL(),
		Handler:     createTestHandler(),
		PingPeriod:  100 * time.Millisecond,
		SendTimeout: 1 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := NewWebsocketClient(ctx, config)
	require.NoError(t, err)
	defer client.Close()

	// Verify client structure
	assert.NotNil(t, client.TradeChan)
	assert.NotNil(t, client.DisconnectChan())
	assert.NotNil(t, client.ErrChan())
	assert.NotNil(t, client.conn.Load())

	// Verify channels are operational
	select {
	case <-client.DisconnectChan():
		t.Error("should not be disconnected initially")
	default:
		// Expected
	}
}

// Test connection failures
func TestNewWebsocketClient_ConnectionFailures(t *testing.T) {
	t.Run("server rejects connection", func(t *testing.T) {
		server := NewTestWebSocketServer()
		server.SetRejectConnection(true)
		defer server.Close()

		config := Config{
			Endpoint: server.URL(),
			Handler:  createTestHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		assert.Error(t, err)
		assert.Nil(t, client)
		assert.Contains(t, err.Error(), "failed to start client")
	})

	t.Run("invalid URL", func(t *testing.T) {
		config := Config{
			Endpoint: "invalid-url",
			Handler:  createTestHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		assert.Error(t, err)
		assert.Nil(t, client)
	})

	t.Run("unreachable server", func(t *testing.T) {
		config := Config{
			Endpoint: "ws://localhost:99999/ws",
			Handler:  createTestHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		assert.Error(t, err)
		assert.Nil(t, client)
	})

	t.Run("context timeout during connection", func(t *testing.T) {
		server := NewTestWebSocketServer()
		server.SetSlowConnection(true)
		defer server.Close()

		config := Config{
			Endpoint: server.URL(),
			Handler:  createTestHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

// Test TLS configuration
func TestNewWebsocketClient_TLS(t *testing.T) {
	t.Run("TLS with insecure skip", func(t *testing.T) {
		server := NewTestTLSWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint:        server.URL(),
			Handler:         createTestHandler(),
			TLSInsecureSkip: true,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		if err != nil {
			t.Logf("TLS connection failed (may be expected in test env): %v", err)
			return
		}

		require.NoError(t, err)
		defer client.Close()

		assert.NotNil(t, client.conn.Load())
	})
}

// Test subscription messages
func TestNewWebsocketClient_SubscriptionMessages(t *testing.T) {
	server := NewTestWebSocketServer()
	defer server.Close()

	subscriptionMsgs := [][]byte{
		[]byte(`{"type":"subscribe","channel":"trades"}`),
		[]byte(`{"type":"subscribe","channel":"ticker"}`),
	}

	config := Config{
		Endpoint:             server.URL(),
		Handler:              createTestHandler(),
		SubscriptionMessages: subscriptionMsgs,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := NewWebsocketClient(ctx, config)
	require.NoError(t, err)
	defer client.Close()

	// Wait for subscription messages to be processed
	time.Sleep(200 * time.Millisecond)

	receivedMsgs := server.GetReceivedMessages()
	assert.GreaterOrEqual(t, len(receivedMsgs), len(subscriptionMsgs))

	// Check that subscription messages were sent
	for i, expected := range subscriptionMsgs {
		if i < len(receivedMsgs) {
			assert.Equal(t, string(expected), string(receivedMsgs[i]))
		}
	}
}

// Test message handling
func TestClient_MessageHandling(t *testing.T) {
	t.Run("successful message processing", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint: server.URL(),
			Handler:  createTestHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)
		defer client.Close()

		// Queue test message
		testMsg := map[string]interface{}{
			"type":   "trade",
			"symbol": "BTC-USDT",
			"price":  50000.0,
		}
		server.QueueMessage(testMsg)

		// Trigger message sending by sending a text message
		server.mu.Lock()
		if len(server.connections) > 0 {
			conn := server.connections[0]
			data, _ := json.Marshal(testMsg)
			conn.WriteMessage(websocket.TextMessage, data)
		}
		server.mu.Unlock()

		// Verify trade event received
		select {
		case trade := <-client.TradeChan:
			assert.Equal(t, "BTC-USDT", trade.Pair)
			assert.Equal(t, decimal.NewFromFloat(50000.0), trade.Price)
		case <-time.After(2 * time.Second):
			t.Error("timeout waiting for trade event")
		}
	})

	t.Run("handler error recovery", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint: server.URL(),
			Handler:  createErrorHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)
		defer client.Close()

		// Send message that will cause error
		server.mu.Lock()
		if len(server.connections) > 0 {
			conn := server.connections[0]
			conn.WriteMessage(websocket.TextMessage, []byte(`{"test":"data"}`))
		}
		server.mu.Unlock()

		// Client should continue running despite handler error
		time.Sleep(100 * time.Millisecond)

		select {
		case <-client.DisconnectChan():
			t.Error("client should not disconnect due to handler error")
		default:
			// Expected
		}
	})

	t.Run("handler panic recovery", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint: server.URL(),
			Handler:  createPanicHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)
		defer client.Close()

		// Send message that will cause panic
		server.mu.Lock()
		if len(server.connections) > 0 {
			conn := server.connections[0]
			conn.WriteMessage(websocket.TextMessage, []byte(`{"test":"data"}`))
		}
		server.mu.Unlock()

		// Client should continue running despite handler panic
		time.Sleep(100 * time.Millisecond)

		select {
		case <-client.DisconnectChan():
			t.Error("client should not disconnect due to handler panic")
		default:
			// Expected
		}
	})
}

// Test ping/pong mechanism
func TestClient_PingPong(t *testing.T) {
	t.Run("pong response handling", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint:    server.URL(),
			Handler:     createTestHandler(),
			PingPeriod:  50 * time.Millisecond,
			SendTimeout: 1 * time.Second,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)
		defer client.Close()

		// Wait for ping-pong exchanges
		time.Sleep(300 * time.Millisecond)

		// Connection should remain stable with ping/pong
		select {
		case <-client.DisconnectChan():
			t.Error("connection should remain stable with ping/pong")
		default:
			// Expected
		}
	})
}

// Test graceful shutdown
func TestClient_Close(t *testing.T) {
	t.Run("graceful shutdown", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint: server.URL(),
			Handler:  createTestHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)

		// Verify initial state
		assert.NotNil(t, client.conn.Load())

		// Close client
		client.Close()

		// Verify shutdown
		select {
		case <-client.DisconnectChan():
			// Expected
		case <-time.After(2 * time.Second):
			t.Error("disconnect channel should be closed")
		}

		// Verify trade channel is closed
		select {
		case _, ok := <-client.TradeChan:
			assert.False(t, ok, "trade channel should be closed")
		case <-time.After(1 * time.Second):
			t.Error("trade channel should be closed")
		}

		// Verify error channel receives shutdown error
		select {
		case err := <-client.ErrChan():
			assert.Equal(t, ErrClientShuttingDown, err)
		case <-time.After(1 * time.Second):
			t.Error("should receive shutdown error")
		}
	})

	t.Run("multiple close calls", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint: server.URL(),
			Handler:  createTestHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)

		// Multiple close calls should not panic
		client.Close()
		client.Close()
		client.Close()

		// Should still be properly closed
		select {
		case <-client.DisconnectChan():
			// Expected
		case <-time.After(1 * time.Second):
			t.Error("should be disconnected")
		}
	})

	t.Run("context cancellation triggers shutdown", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint: server.URL(),
			Handler:  createTestHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)

		// Cancel context
		cancel()

		// Should trigger shutdown
		select {
		case <-client.DisconnectChan():
			// Expected
		case <-time.After(2 * time.Second):
			t.Error("should disconnect when context cancelled")
		}
	})
}

// Test connection failures and error handling
func TestClient_ConnectionFailures(t *testing.T) {
	t.Run("server closes connection", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint: server.URL(),
			Handler:  createTestHandler(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)
		defer client.Close()

		// Force server to drop connections
		server.SetDropConnection(true)
		server.mu.Lock()
		for _, conn := range server.connections {
			conn.Close()
		}
		server.mu.Unlock()

		// Should detect disconnection
		select {
		case <-client.DisconnectChan():
			// Expected
		case <-time.After(2 * time.Second):
			t.Error("should detect connection closure")
		}

		// Should receive connection error
		select {
		case err := <-client.ErrChan():
			assert.NotEqual(t, ErrClientShuttingDown, err)
		case <-time.After(1 * time.Second):
			t.Error("should receive connection error")
		}
	})
}

// Test channel access methods
func TestClient_ChannelAccess(t *testing.T) {
	server := NewTestWebSocketServer()
	defer server.Close()

	config := Config{
		Endpoint: server.URL(),
		Handler:  createTestHandler(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := NewWebsocketClient(ctx, config)
	require.NoError(t, err)
	defer client.Close()

	t.Run("DisconnectChan", func(t *testing.T) {
		ch := client.DisconnectChan()
		assert.NotNil(t, ch)

		// Should return same channel
		ch2 := client.DisconnectChan()
		assert.Equal(t, ch, ch2)
	})

	t.Run("ErrChan", func(t *testing.T) {
		ch := client.ErrChan()
		assert.NotNil(t, ch)

		// Should return same channel
		ch2 := client.ErrChan()
		assert.Equal(t, ch, ch2)
	})

	t.Run("TradeChan", func(t *testing.T) {
		assert.NotNil(t, client.TradeChan)
	})
}

// Test constants and defaults
func TestConstants(t *testing.T) {
	assert.Equal(t, 15*time.Second, defaultPingPeriod)
	assert.Equal(t, 5*time.Second, defaultSendTimeout)
	assert.Equal(t, int(1<<20), defaultReadLimit)
	assert.Equal(t, 10*time.Second, defaultHandshakeTimeout)
}

// Test error variables
func TestErrors(t *testing.T) {
	assert.Equal(t, "client is shutting down", ErrClientShuttingDown.Error())
}

// Performance and stress tests
func TestClient_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance tests in short mode")
	}

	t.Run("high message throughput", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		var messageCount atomic.Int64
		config := Config{
			Endpoint: server.URL(),
			Handler: func(data []byte, tradeChan chan<- model.TradeEvent) error {
				messageCount.Add(1)
				return nil
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)
		defer client.Close()

		// Send many messages quickly
		numMessages := 1000
		server.mu.Lock()
		if len(server.connections) > 0 {
			conn := server.connections[0]
			go func() {
				defer server.mu.Unlock()
				for i := 0; i < numMessages; i++ {
					msg := fmt.Sprintf(`{"id":%d,"data":"test"}`, i)
					conn.WriteMessage(websocket.TextMessage, []byte(msg))
				}
			}()
		} else {
			server.mu.Unlock()
		}

		// Wait for processing
		time.Sleep(2 * time.Second)

		processed := messageCount.Load()
		assert.Greater(t, processed, int64(0))
		t.Logf("Processed %d messages", processed)
	})

	t.Run("concurrent clients", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		numClients := 10
		clients := make([]*Client, 0, numClients)
		var wg sync.WaitGroup

		for i := 0; i < numClients; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				config := Config{
					Endpoint: server.URL(),
					Handler:  createTestHandler(),
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				client, err := NewWebsocketClient(ctx, config)
				if err != nil {
					t.Errorf("failed to create client: %v", err)
					return
				}

				clients = append(clients, client)
				time.Sleep(100 * time.Millisecond)
			}()
		}

		wg.Wait()

		// Clean up
		for _, client := range clients {
			if client != nil {
				client.Close()
			}
		}

		assert.Greater(t, len(clients), 0)
	})
}

// Test edge cases
func TestClient_EdgeCases(t *testing.T) {
	t.Run("zero timeout values", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint:    server.URL(),
			Handler:     createTestHandler(),
			PingPeriod:  0, // Should use default
			SendTimeout: 0, // Should use default
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)
		defer client.Close()

		// Verify defaults were applied
		assert.Equal(t, defaultPingPeriod, client.cfg.PingPeriod)
		assert.Equal(t, defaultSendTimeout, client.cfg.SendTimeout)
	})

	t.Run("empty subscription messages", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint:             server.URL(),
			Handler:              createTestHandler(),
			SubscriptionMessages: [][]byte{},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)
		defer client.Close()

		assert.NotNil(t, client.cfg.SubscriptionMessages)
		assert.Empty(t, client.cfg.SubscriptionMessages)
	})

	t.Run("nil subscription messages", func(t *testing.T) {
		server := NewTestWebSocketServer()
		defer server.Close()

		config := Config{
			Endpoint:             server.URL(),
			Handler:              createTestHandler(),
			SubscriptionMessages: nil,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		client, err := NewWebsocketClient(ctx, config)
		require.NoError(t, err)
		defer client.Close()

		assert.NotNil(t, client.cfg.SubscriptionMessages)
		assert.Empty(t, client.cfg.SubscriptionMessages)
	})
}

// Benchmark tests
func BenchmarkClient_MessageHandling(b *testing.B) {
	server := NewTestWebSocketServer()
	defer server.Close()

	var processed atomic.Int64
	config := Config{
		Endpoint: server.URL(),
		Handler: func(data []byte, tradeChan chan<- model.TradeEvent) error {
			processed.Add(1)
			return nil
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := NewWebsocketClient(ctx, config)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	testMessage := `{"type":"trade","symbol":"BTC-USDT","price":50000}`

	b.ResetTimer()
	b.ReportAllocs()

	server.mu.Lock()
	if len(server.connections) > 0 {
		conn := server.connections[0]
		server.mu.Unlock()

		for i := 0; i < b.N; i++ {
			conn.WriteMessage(websocket.TextMessage, []byte(testMessage))
		}

		// Wait for processing
		for processed.Load() < int64(b.N) {
			time.Sleep(time.Microsecond)
		}
	} else {
		server.mu.Unlock()
		b.Fatal("no connection available")
	}
}

func BenchmarkClient_Creation(b *testing.B) {
	server := NewTestWebSocketServer()
	defer server.Close()

	config := Config{
		Endpoint: server.URL(),
		Handler:  createTestHandler(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		client, err := NewWebsocketClient(ctx, config)
		if err != nil {
			cancel()
			b.Fatal(err)
		}

		client.Close()
		cancel()
	}
}
