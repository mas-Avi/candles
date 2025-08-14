// Package exchange provides cryptocurrency exchange connectors for real-time trade data streaming.
//
// This file implements the OKX exchange connector, which provides WebSocket-based access to
// real-time trade events from OKX (formerly OKEx), one of the world's largest cryptocurrency
// derivatives exchanges.
package exchange

import (
	"candles/internal/model"
	"candles/internal/utils"
	"candles/internal/websocket"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-playground/validator/v10"
	json "github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"
)

var (
	// defaultOkxConfig provides sensible defaults for OKX exchange connections.
	defaultOkxConfig = ExchangeConfig{
		BaseURL:    "wss://ws.okx.com:8443/ws/v5/public",
		MaxSymbols: 10,
	}
)

// OkxConnector implements the ExchangeConnector interface for OKX exchange.
//
// This connector provides WebSocket-based access to real-time trade data from OKX,
// handling the exchange-specific protocol details, message formats, and data validation.
// It manages the complete lifecycle from subscription to trade event delivery.
type OkxConnector struct {
	// config stores the validated exchange configuration.
	config ExchangeConfig

	// validate provides field validation for incoming OKX messages.
	validate *validator.Validate
}

// subscription defines the structure for OKX WebSocket subscription requests.
//
// This structure represents the JSON payload sent to OKX's WebSocket API to
// establish subscriptions for real-time trade data. It follows OKX's API v5
// subscription protocol exactly.
//
// Protocol Details:
//   - "op": Operation type, always "subscribe" for establishing subscriptions
//   - "args": Array of subscription arguments, one per trading pair
//
// Subscription Arguments:
//
//	Each arg contains:
//	- "channel": Data channel type, "trades" for trade events
//	- "instId": Instrument identifier (trading pair like "BTC-USDT")
//
// Example JSON:
//
//	{
//	  "op": "subscribe",
//	  "args": [
//	    {"channel": "trades", "instId": "BTC-USDT"},
//	    {"channel": "trades", "instId": "ETH-USDT"}
//	  ]
//	}
type subscription struct {
	// Op specifies the WebSocket operation type.
	//
	// For trade subscriptions, this is always "subscribe". Other possible
	// values in OKX API include "unsubscribe" for removing subscriptions.
	//
	// JSON Field: "op"
	// Required: Yes
	// Valid Values: "subscribe", "unsubscribe"
	Op string `json:"op"`

	// Args contains the subscription parameters for each trading pair.
	//
	// This array holds one subscription argument per trading pair, where
	// each argument specifies the channel type and instrument identifier.
	// Using interface{} allows flexibility for different argument types.
	//
	// JSON Field: "args"
	// Required: Yes
	// Type: Array of objects
	// Content: {"channel": "trades", "instId": "PAIR-NAME"}
	Args []interface{} `json:"args"`
}

// okxTradeMessage represents the structure of incoming trade messages from OKX.
//
// This structure maps exactly to OKX's WebSocket API v5 trade message format,
// handling the nested JSON structure and array-based trade data delivery.
// OKX typically sends multiple trades in a single message for efficiency.
//
// Message Structure:
//   - "arg": Subscription context (channel and instrument)
//   - "data": Array of trade events (usually 1-10 trades per message)
//
// Validation Strategy:
//   - Struct tags define validation rules for each field
//   - Required fields must be present and non-empty
//   - Numeric fields validated as parseable strings
//   - Enum fields validated against allowed values
//   - Nested validation applied to data array elements
//
// Example JSON:
//
//	{
//	  "arg": {"channel": "trades", "instId": "BTC-USDT"},
//	  "data": [
//	    {
//	      "instId": "BTC-USDT",
//	      "tradeId": "123456789",
//	      "px": "50000.00",
//	      "sz": "0.001",
//	      "side": "buy",
//	      "ts": "1640995200000"
//	    }
//	  ]
//	}
type okxTradeMessage struct {
	// Arg contains subscription context information.
	//
	// This nested structure identifies which subscription generated this
	// message, allowing proper routing and validation of trade data.
	//
	// JSON Field: "arg"
	// Required: Yes
	// Validation: Must contain valid channel and instId
	Arg struct {
		// Channel identifies the data stream type.
		//
		// For trade messages, this must always be "trades". This validation
		// ensures we're processing the correct message type and haven't
		// received unexpected data.
		//
		// JSON Field: "channel"
		// Required: Yes
		// Valid Values: "trades"
		// Validation: Must equal "trades" exactly
		Channel string `json:"channel" validate:"required,eq=trades"`

		// InstID is the instrument identifier (trading pair).
		//
		// This field specifies which trading pair the trade data relates to,
		// using OKX's standard format like "BTC-USDT", "ETH-BTC", etc.
		//
		// JSON Field: "instId"
		// Required: Yes
		// Format: "BASE-QUOTE" (e.g., "BTC-USDT")
		// Validation: Must be present and non-empty
		InstID string `json:"instId" validate:"required"`
	} `json:"arg" validate:"required"`

	// Data contains the array of trade events in this message.
	//
	// OKX typically batches multiple trades into a single WebSocket message
	// for efficiency. Each element represents one executed trade with full
	// details including price, quantity, and timing information.
	//
	// JSON Field: "data"
	// Required: Yes
	// Type: Array of trade objects
	// Validation: Must contain at least one trade, all trades validated
	Data []struct {
		// InstID is the trading pair for this specific trade.
		//
		// Should match the InstID in the Arg section. Provides redundancy
		// and allows for message validation and routing.
		//
		// JSON Field: "instId"
		// Required: Yes
		// Example: "BTC-USDT"
		InstID string `json:"instId" validate:"required"`

		// TradeID is OKX's unique identifier for this trade.
		//
		// This field provides a unique reference for each trade event,
		// useful for deduplication and audit trails.
		//
		// JSON Field: "tradeId"
		// Required: Yes
		// Type: String (numeric)
		// Example: "123456789"
		TradeID string `json:"tradeId" validate:"required"`

		// Price is the execution price as a string.
		//
		// OKX sends prices as strings to preserve full precision and
		// avoid floating-point representation issues. Must be parsed
		// to decimal.Decimal for financial calculations.
		//
		// JSON Field: "px"
		// Required: Yes
		// Type: Numeric string
		// Example: "50000.12345678"
		// Validation: Must be parseable as a number
		Price string `json:"px" validate:"required,numeric"`

		// Size is the trade quantity as a string.
		//
		// Like Price, Size is provided as a string to maintain precision.
		// Represents the amount of the base currency traded.
		//
		// JSON Field: "sz"
		// Required: Yes
		// Type: Numeric string
		// Example: "0.001"
		// Validation: Must be parseable as a positive number
		Size string `json:"sz" validate:"required,numeric"`

		// Side indicates whether this was a buy or sell trade.
		//
		// Represents the taker side of the trade (the side that matched
		// against an existing order in the order book).
		//
		// JSON Field: "side"
		// Required: Yes
		// Valid Values: "buy", "sell"
		// Validation: Must be exactly "buy" or "sell"
		Side string `json:"side" validate:"required,oneof=buy sell"`

		// TS is the trade timestamp in milliseconds as a string.
		//
		// OKX provides timestamps as string representations of Unix
		// milliseconds. Must be parsed to int64 and converted to time.Time.
		//
		// JSON Field: "ts"
		// Required: Yes
		// Type: Numeric string (milliseconds since Unix epoch)
		// Example: "1640995200000"
		// Validation: Must be parseable as integer
		TS string `json:"ts" validate:"required,numeric"`
	} `json:"data" validate:"required,min=1,dive"`
}

// NewOkxConnector creates a new OKX exchange connector with the specified configuration.
//
// This constructor initializes all components required for OKX WebSocket communication:
// configuration validation, logger setup, and message validator initialization.
// It follows a fail-fast approach where any configuration error prevents connector creation.
func NewOkxConnector(cfg *ExchangeConfig) (*OkxConnector, error) {
	// Apply default configuration if none provided
	if cfg == nil {
		cfg = &defaultOkxConfig
	}

	// Validate configuration against requirements
	if err := validateConfig(cfg, &defaultOkxConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	// Initialize connector with validated configuration
	return &OkxConnector{
		config:   *cfg,
		validate: validator.New(),
	}, nil
}

// SubscribeToTrades establishes WebSocket subscriptions for trade events from specified trading pairs.
//
// This method implements the ExchangeConnector interface, handling the complete subscription
// lifecycle from parameter validation through WebSocket connection establishment to trade
// event delivery. It follows OKX's WebSocket API v5 protocol for trade data subscriptions.
func (oc *OkxConnector) SubscribeToTrades(ctx context.Context, pairs []string) (<-chan model.TradeEvent, error) {
	// Validate trading pairs against connector limits
	if err := utils.ValidatePairs(pairs, oc.config.MaxSymbols); err != nil {
		return nil, err
	}

	// Build OKX-specific subscription message
	msg, err := oc.buildSubscriptionMessage(pairs)
	if err != nil {
		log.Error().Err(err).Msg("failed to marshal subscription message")
		return nil, err
	}

	// Create WebSocket client with OKX configuration
	client, err := websocket.NewWebsocketClient(ctx, websocket.Config{
		Endpoint:             oc.config.BaseURL,
		Handler:              oc.handleTradeMessage,
		SubscriptionMessages: [][]byte{msg},
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to create OKX WebSocket client")
		return nil, err
	}

	return client.TradeChan, nil
}

// buildSubscriptionMessage creates an OKX WebSocket subscription request for the specified trading pairs.
//
// This method constructs the JSON payload required by OKX's WebSocket API v5 to establish
// trade data subscriptions. It formats the subscription according to OKX's exact protocol
// requirements and handles multiple trading pairs in a single subscription request.
func (oc *OkxConnector) buildSubscriptionMessage(pairs []string) ([]byte, error) {
	// Pre-allocate args slice with known capacity
	args := make([]interface{}, 0, len(pairs))

	// Build subscription argument for each trading pair
	for _, p := range pairs {
		args = append(args, map[string]string{
			"channel": "trades",
			"instId":  p,
		})
	}

	// Create subscription request structure
	sub := subscription{
		Op:   "subscribe",
		Args: args,
	}

	// Marshal to JSON for WebSocket transmission
	return json.Marshal(sub)
}

// handleTradeMessage processes incoming WebSocket messages from OKX and converts them to TradeEvents.
func (oc *OkxConnector) handleTradeMessage(raw []byte, tradeChan chan<- model.TradeEvent) error {
	var msg okxTradeMessage

	// Parse JSON message structure
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.Error().Err(err).Msg("failed to unmarshal OKX trade message")
		return err
	}

	// Validate message structure and required fields
	if err := oc.validate.Struct(&msg); err != nil {
		log.Error().Err(err).Interface("msg", msg).Msg("validation failed for OKX trade message")
		return err
	}

	// Process each trade in the batch
	for _, d := range msg.Data {
		// Convert timestamp from string to int64 milliseconds
		tsInt, err := strconv.ParseInt(d.TS, 10, 64)
		if err != nil {
			log.Error().Str("ts", d.TS).Msg("invalid timestamp format")
			return err
		}

		// Convert price from string to decimal with full precision
		price, err := decimal.NewFromString(d.Price)
		if err != nil {
			log.Error().Err(err).Str("price", d.Price).Msg("invalid trade price")
			return err
		}

		// Convert quantity from string to decimal with full precision
		quantity, err := decimal.NewFromString(d.Size)
		if err != nil {
			log.Error().Err(err).Str("size", d.Size).Msg("invalid trade quantity")
			return err
		}

		// Create normalized TradeEvent and send to channel
		tradeChan <- model.TradeEvent{
			Pair:      d.InstID,
			Price:     price,
			Quantity:  quantity,
			Timestamp: time.UnixMilli(tsInt),
			Exchange:  model.OkxExchange,
		}
	}

	return nil
}
