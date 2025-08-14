// Package websocket provides a robust WebSocket client implementation for cryptocurrency exchange connections.
//
// This package offers a production-ready WebSocket client designed specifically for high-frequency
// trading data streams from cryptocurrency exchanges. It provides comprehensive lifecycle management,
// error handling, and connection resilience features.
package websocket

import (
	"candles/internal/model"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
)

const (
	// defaultPingPeriod defines the default interval for sending WebSocket ping messages.
	defaultPingPeriod = 15 * time.Second

	// defaultSendTimeout defines the default timeout for WebSocket write operations.
	defaultSendTimeout = 5 * time.Second

	// defaultReadLimit defines the maximum size of incoming WebSocket messages.
	defaultReadLimit = 1 << 20 // 1MB

	// defaultHandshakeTimeout defines the maximum time allowed for WebSocket handshake.
	defaultHandshakeTimeout = 10 * time.Second
)

// Common errors returned by the WebSocket client
var (
	// ErrClientShuttingDown indicates that the client is in the process of shutting down.
	ErrClientShuttingDown = errors.New("client is shutting down")
)

// Config defines settings for the WebSocket client.
type Config struct {
	// Endpoint is the WebSocket URL to connect to.
	// Required: This field must be provided and non-empty.
	Endpoint string

	// Handler is the function called for each incoming WebSocket message.
	// Required: This field must be provided and non-nil.
	Handler func([]byte, chan<- model.TradeEvent) error

	// TLSInsecureSkip disables TLS certificate verification.
	TLSInsecureSkip bool

	// PingPeriod is the interval between WebSocket ping messages.
	PingPeriod time.Duration

	// SendTimeout is the maximum time allowed for WebSocket write operations.
	SendTimeout time.Duration

	// SubscriptionMessages contains messages to send immediately after connection.
	SubscriptionMessages [][]byte
}

// Client wraps a websocket.Conn with lifecycle and message handling logic.
//
// The Client provides a high-level interface for WebSocket communication with
// cryptocurrency exchanges. It manages the complete lifecycle from connection
// establishment through message processing to graceful shutdown.
type Client struct {
	// conn stores the active WebSocket connection using atomic operations.
	conn atomic.Value // stores *websocket.Conn

	// TradeChan delivers processed trade events to consumers.
	TradeChan chan model.TradeEvent

	// disconnect signals when the WebSocket connection is lost.
	disconnect chan struct{}

	// errChan reports fatal errors that cause connection termination.
	errChan chan error

	// cfg holds the client configuration.
	cfg *Config

	// ctx is the cancellation context for coordinating shutdown.
	ctx context.Context

	// cancel is the function to trigger context cancellation.
	cancel context.CancelFunc

	// once ensures Close() is only executed once.
	once sync.Once

	// wg coordinates goroutine shutdown.
	wg sync.WaitGroup
}

// NewWebsocketClient returns a configured WebSocket client and immediately starts subscriptions.
//
// This function creates and initializes a new WebSocket client with the provided configuration.
// It performs comprehensive validation, establishes the WebSocket connection, sends any
// subscription messages, and starts all background goroutines for message processing.
func NewWebsocketClient(ctx context.Context, cfg Config) (*Client, error) {
	// Validate required configuration fields
	if cfg.Endpoint == "" {
		return nil, errors.New("endpoint URL is required")
	}
	if cfg.Handler == nil {
		return nil, errors.New("message handler is required")
	}

	// Apply defaults for optional fields
	if cfg.SubscriptionMessages == nil {
		cfg.SubscriptionMessages = [][]byte{}
	}
	if cfg.PingPeriod == 0 {
		cfg.PingPeriod = defaultPingPeriod
	}
	if cfg.SendTimeout == 0 {
		cfg.SendTimeout = defaultSendTimeout
	}

	// Create cancellable context for client lifecycle
	ctx, cancel := context.WithCancel(ctx)

	// Initialize client structure
	client := &Client{
		cfg:        &cfg,
		ctx:        ctx,
		cancel:     cancel,
		disconnect: make(chan struct{}),
		errChan:    make(chan error, 1),
		TradeChan:  make(chan model.TradeEvent, 1000),
	}

	// Start client and establish connection
	if err := client.run(cfg.SubscriptionMessages); err != nil {
		cancel() // Clean up context on failure
		return nil, fmt.Errorf("failed to start client: %w", err)
	}

	return client, nil
}

// run establishes the WebSocket connection and manages lifecycle.
//
// This internal method handles the core client initialization process:
// connection establishment, configuration, subscription message sending,
// and goroutine startup. It's called once during client creation.
func (c *Client) run(subMsgs [][]byte) error {
	logger := log.With().
		Str("endpoint", c.cfg.Endpoint).
		Str("component", "run").
		Logger()

	logger.Info().Msg("starting WebSocket client")

	// Establish WebSocket connection
	conn, err := c.dial(c.ctx)
	if err != nil {
		return fmt.Errorf("initial dial failed: %w", err)
	}

	// Ensure connection is cleaned up if initialization fails
	defer func() {
		if err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				logger.Warn().Err(closeErr).Msg("error closing connection during cleanup")
			}
		}
	}()

	// Store connection atomically for thread-safe access
	c.conn.Store(conn)

	// Configure connection parameters
	conn.SetReadLimit(defaultReadLimit)
	conn.SetPongHandler(func(appData string) error {
		// Update read deadline when pong is received
		deadline := time.Now().Add(c.cfg.PingPeriod * 2)
		if err := conn.SetReadDeadline(deadline); err != nil {
			logger.Warn().Err(err).Msg("failed to set read deadline in pong handler")
		}
		return nil
	})

	// Send initial subscription messages
	for _, msg := range subMsgs {
		if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
			logger.Error().Err(err).Msg("subscription error")
			return err
		}
	}

	// Start background goroutines with WaitGroup tracking
	c.wg.Add(3)
	go func() {
		defer c.wg.Done()
		c.readLoop()
	}()
	go func() {
		defer c.wg.Done()
		c.pingLoop()
	}()
	go func() {
		defer c.wg.Done()
		c.shutdownListener()
	}()

	return nil
}

// readLoop continuously reads messages from the WebSocket connection.
//
// This method runs in its own goroutine and forms the core of the client's
// message processing capability. It reads messages from the WebSocket
// connection and delegates processing to the configured Handler function.
func (c *Client) readLoop() {
	conn := c.conn.Load().(*websocket.Conn)
	logger := log.With().
		Str("endpoint", c.cfg.Endpoint).
		Str("component", "readLoop").
		Logger()

	logger.Info().Msg("starting read loop")
	defer func() {
		logger.Info().Msg("read loop exiting")
		close(c.disconnect) // Signal disconnect to consumers
		close(c.TradeChan)  // Close sending channel

		// Try to send error if channel not full
		select {
		case c.errChan <- ErrClientShuttingDown:
		default:
			logger.Debug().Msg("error channel full, skipping error send")
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			logger.Info().Msg("context cancelled, exiting read loop")
			return
		default:
			// Read message from WebSocket
			messageType, data, err := conn.ReadMessage()
			if err != nil {
				// Categorize and log different error types
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					logger.Info().Err(err).Msg("websocket closed normally")
				} else if websocket.IsUnexpectedCloseError(err) {
					logger.Warn().Err(err).Msg("unexpected websocket closure")
				} else {
					logger.Error().Err(err).Msg("read error")
				}

				// Try to send error if channel not full
				select {
				case c.errChan <- err:
				default:
					logger.Warn().Err(err).Msg("error channel full, dropping error")
				}
				return
			}

			// Log message details for debugging
			logger.Debug().
				Int("messageType", messageType).
				Int("bytes", len(data)).
				Msg("received message")

			// Process message with handler (if configured)
			if c.cfg.Handler != nil {
				func() {
					// Recover from handler panics to prevent client crash
					defer func() {
						if r := recover(); r != nil {
							logger.Error().Any("recover", r).Msg("panic in message handler")
						}
					}()

					// Call handler with message data and trade channel
					if err := c.cfg.Handler(data, c.TradeChan); err != nil {
						log.Error().Err(err).Msg("Error handling trade event")
					}
				}()
			}
		}
	}
}

// pingLoop sends periodic ping messages to keep the connection alive.
//
// This method runs in its own goroutine and implements the WebSocket
// keepalive mechanism. It sends ping messages at regular intervals
// to detect connection failures and prevent idle timeouts.
func (c *Client) pingLoop() {
	ticker := time.NewTicker(c.cfg.PingPeriod)
	defer ticker.Stop()

	logger := log.With().
		Str("endpoint", c.cfg.Endpoint).
		Str("component", "pingLoop").
		Logger()

	logger.Info().Dur("period", c.cfg.PingPeriod).Msg("starting ping loop")
	defer logger.Info().Msg("ping loop exiting")

	for {
		select {
		case <-ticker.C:
			// Get connection safely on each iteration
			connVal := c.conn.Load()
			if connVal == nil {
				logger.Debug().Msg("connection not available for ping")
				continue
			}
			conn := connVal.(*websocket.Conn)

			// Set write deadline to prevent hanging
			deadline := time.Now().Add(c.cfg.SendTimeout)
			if err := conn.SetWriteDeadline(deadline); err != nil {
				logger.Warn().Err(err).Msg("failed to set write deadline")
				continue
			}

			// Send ping message
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				logger.Warn().Err(err).Msg("ping error")
			} else {
				logger.Debug().Msg("ping sent")
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// shutdownListener waits for context cancellation and closes connection.
//
// This method runs in its own goroutine and provides a clean shutdown
// mechanism when the context is cancelled. It ensures that shutdown
// is initiated promptly when requested.
func (c *Client) shutdownListener() {
	<-c.ctx.Done()
	log.Info().Msg("context cancelled, shutting down WebSocket client")
	c.Close()
}

// Close gracefully shuts down the client.
//
// This method implements a comprehensive shutdown procedure that ensures
// all resources are properly cleaned up and all goroutines terminate
// gracefully. It can be called multiple times safely.
func (c *Client) Close() {
	c.once.Do(func() {
		logger := log.With().
			Str("endpoint", c.cfg.Endpoint).
			Str("component", "close").
			Logger()

		logger.Info().Msg("initiating graceful shutdown")

		// First cancel context to signal all goroutines
		c.cancel()

		// Then close the websocket connection
		if conn := c.conn.Load(); conn != nil {
			if ws, ok := conn.(*websocket.Conn); ok {
				// Send close frame with normal closure code
				if err := ws.WriteControl(
					websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
					time.Now().Add(time.Second),
				); err != nil {
					logger.Warn().Err(err).Msg("failed to send close frame")
				}

				// Close underlying connection
				if err := ws.Close(); err != nil {
					logger.Warn().Err(err).Msg("error closing websocket connection")
				}
			}
		}

		// Wait for all goroutines to complete
		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			logger.Info().Msg("all goroutines completed")
		case <-time.After(5 * time.Second):
			logger.Warn().Msg("timeout waiting for goroutines to complete")
		}

		logger.Info().Msg("shutdown complete")
	})
}

// dial establishes a WebSocket connection.
//
// This method creates a WebSocket connection to the configured endpoint
// using appropriate settings for cryptocurrency exchange communication.
// It handles proxy configuration, TLS settings, and connection timeouts.
func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	logger := log.With().
		Str("endpoint", c.cfg.Endpoint).
		Bool("tlsInsecureSkip", c.cfg.TLSInsecureSkip).
		Dur("handshakeTimeout", defaultHandshakeTimeout).
		Logger()

	logger.Info().Msg("attempting websocket connection")

	// Configure WebSocket dialer
	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: c.cfg.TLSInsecureSkip},
		HandshakeTimeout: defaultHandshakeTimeout,
	}

	// Establish connection
	conn, resp, err := dialer.DialContext(ctx, c.cfg.Endpoint, make(http.Header))
	if err != nil {
		// Log detailed error information
		if resp != nil {
			logger.Error().
				Err(err).
				Int("statusCode", resp.StatusCode).
				Str("status", resp.Status).
				Msg("connection failed")
		} else {
			logger.Error().Err(err).Msg("connection failed")
		}
		return nil, err
	}

	logger.Info().Msg("websocket connection established")
	return conn, nil
}

// DisconnectChan returns a channel that is closed when the client disconnects.
//
// This method provides a way for consumers to monitor the connection status
// and react to disconnection events. The returned channel is closed when
// the WebSocket connection is lost for any reason.
func (c *Client) DisconnectChan() <-chan struct{} {
	return c.disconnect
}

// ErrChan returns a channel that emits any terminal read errors.
//
// This method provides access to detailed error information when the
// WebSocket connection encounters problems. Errors are sent to this
// channel when they cause connection termination or other serious issues.
func (c *Client) ErrChan() <-chan error {
	return c.errChan
}
