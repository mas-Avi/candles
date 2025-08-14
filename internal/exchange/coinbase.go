// Package exchange provides cryptocurrency exchange connectors for real-time trading data.
//
// The Coinbase connector implements the ExchangeConnector interface to provide
// WebSocket-based streaming of trade events from the Coinbase Pro (Advanced Trade) exchange.
// It handles Coinbase-specific message formats, validation, and data normalization.
//
// Key features:
//   - Real-time trade event streaming via WebSocket with subscription-based model
//   - Comprehensive input validation using struct tags and validator
//   - Financial precision using decimal.Decimal for price/quantity data
//   - RFC3339 timestamp parsing for precise trade timing
//   - Robust error handling with structured logging
//   - Configurable connection parameters
//
// Coinbase WebSocket API differences from other exchanges:
//   - Uses subscription-based model (send subscription message after connection)
//   - Trade events are called "matches" in Coinbase terminology
//   - Symbols are already in hyphenated format (BTC-USD, ETH-USD)
//   - Timestamps are in RFC3339 format with timezone information
//   - Single-level JSON structure (no nested wrapper like Binance)
package exchange

import (
	"candles/internal/model"
	"candles/internal/utils"
	"candles/internal/websocket"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	json "github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

var (
	// defaultCoinbaseConfig provides sensible default configuration values for Coinbase connections.
	defaultCoinbaseConfig = ExchangeConfig{
		BaseURL:    "wss://ws-feed.exchange.coinbase.com",
		MaxSymbols: 10,
	}

	// ErrMessageTypeNotMatch indicates that a received WebSocket message is not a trade match.
	ErrMessageTypeNotMatch = errors.New("message type is not 'match'")
)

// CoinbaseConnector provides a Coinbase-specific implementation of the ExchangeConnector interface.
type CoinbaseConnector struct {
	config   ExchangeConfig      // Configuration parameters for the connector
	validate *validator.Validate // Validator instance for message validation
}

// coinbaseMatch represents a trade execution (match) message from Coinbase WebSocket API.
//
// Coinbase uses the term "match" to describe trade executions where a buy order
// is matched with a sell order. This structure maps directly to Coinbase's
// match message format, using string representations for numeric values
// to preserve precision during JSON parsing.
//
// Message Structure:
//
//	The Coinbase match message is a single-level JSON object containing all
//	trade information directly (no nested wrapper like other exchanges).
//
// Field Validation:
//   - Type must be exactly "match" (other message types are ignored)
//   - Price and Size must be numeric strings for decimal precision
//   - ProductID must be present (trading pair identifier)
//   - Time must be in RFC3339 format with timezone (2006-01-02T15:04:05Z07:00)
//
// Example Coinbase match message:
//
//	{
//		"type": "match",
//		"trade_id": 12345,
//		"maker_order_id": "abc123",
//		"taker_order_id": "def456",
//		"side": "buy",
//		"size": "0.00100000",
//		"price": "50000.00",
//		"product_id": "BTC-USD",
//		"sequence": 987654321,
//		"time": "2023-01-01T12:00:00.123456Z"
//	}
type coinbaseMatch struct {
	Type      string `json:"type" validate:"required,eq=match"`                           // Message type, must be "match"
	Price     string `json:"price" validate:"required,numeric"`                           // Trade execution price as string
	Size      string `json:"size" validate:"required,numeric"`                            // Trade quantity as string
	ProductID string `json:"product_id" validate:"required"`                              // Trading pair (e.g., "BTC-USD")
	Time      string `json:"time" validate:"required,datetime=2006-01-02T15:04:05Z07:00"` // RFC3339 timestamp with timezone
}

// NewCoinbaseConnector creates a new Coinbase connector with the specified configuration.
//
// If no configuration is provided (cfg is nil), the connector will use default
// configuration values suitable for most use cases. The configuration is validated
// against the defaults to ensure all required fields are present and valid.
func NewCoinbaseConnector(cfg *ExchangeConfig) (*CoinbaseConnector, error) {
	if cfg == nil {
		cfg = &defaultCoinbaseConfig
	}

	if err := validateConfig(cfg, &defaultCoinbaseConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	return &CoinbaseConnector{
		config:   *cfg,
		validate: validator.New(),
	}, nil
}

// SubscribeToTrades establishes a WebSocket connection to Coinbase and subscribes to
// trade events (matches) for the specified trading pairs.
func (cc *CoinbaseConnector) SubscribeToTrades(ctx context.Context, pairs []string) (<-chan model.TradeEvent, error) {
	if err := utils.ValidatePairs(pairs, cc.config.MaxSymbols); err != nil {
		return nil, err
	}

	// Subscription message
	msg, err := cc.buildSubscriptionMessage(pairs)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal")
		return nil, err
	}

	client, err := websocket.NewWebsocketClient(ctx, websocket.Config{
		Endpoint:             cc.config.BaseURL,
		Handler:              cc.handleTradeMessage,
		SubscriptionMessages: [][]byte{msg},
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to create coinbase WebSocket client")
		return nil, err
	}

	return client.TradeChan, nil
}

// buildSubscriptionMessage constructs the JSON subscription message for Coinbase WebSocket API.
//
// Coinbase uses a subscription-based model where clients must send a subscription
// message after establishing the WebSocket connection to specify which data streams
// they want to receive.
//
// Subscription Message Format:
//
//	The message subscribes to the "matches" channel for the specified trading pairs.
//	The "matches" channel provides real-time trade execution data.
//
// Message Structure:
//
//	{
//	  "type": "subscribe",
//	  "product_ids": ["BTC-USD", "ETH-USD"],
//	  "channels": ["matches"]
//	}
//
// Channel Types:
//   - "matches": Real-time trade executions (what we want)
//   - "level2": Order book updates
//   - "heartbeat": Connection keepalive messages
//   - "ticker": Price ticker updates
func (cc *CoinbaseConnector) buildSubscriptionMessage(pairs []string) ([]byte, error) {
	subMsg := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": pairs,
		"channels":    []string{"matches"},
	}
	return json.Marshal(subMsg)
}

// handleTradeMessage processes raw WebSocket messages from Coinbase and converts them
// to standardized TradeEvent structures.
func (cc *CoinbaseConnector) handleTradeMessage(raw []byte, tradeChan chan<- model.TradeEvent) error {
	var msg coinbaseMatch

	// Decode JSON
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.Error().Err(err).Msg("failed to unmarshal match message")
		return err
	}

	// Validate fields
	if err := cc.validate.Struct(&msg); err != nil {
		log.Error().Err(err).Interface("msg", msg).Msg("validation failed for match message")
		return err
	}

	// Parse time
	t, err := time.Parse(time.RFC3339, msg.Time)
	if err != nil {
		log.Error().Str("time", msg.Time).Msg("unable to parse time string")
		return err
	}

	price, err := decimal.NewFromString(msg.Price)
	if err != nil {
		log.Error().Err(err).Msg("invalid trade price")
		return err
	}

	quantity, err := decimal.NewFromString(msg.Size)
	if err != nil {
		log.Error().Err(err).Msg("invalid trade quantity")
		return err
	}

	// Send trade event
	tradeChan <- model.TradeEvent{
		Pair:      msg.ProductID,
		Timestamp: t.UTC(),
		Price:     price,
		Quantity:  quantity,
		Exchange:  model.CoinbaseExchange,
	}

	return nil
}
