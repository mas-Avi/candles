// Package exchange provides cryptocurrency exchange connectors for real-time trading data.
//
// The Binance connector implements the ExchangeConnector interface to provide
// WebSocket-based streaming of trade events from the Binance cryptocurrency exchange.
// It handles Binance-specific message formats, validation, and symbol normalization.
//
// Key features:
//   - Real-time trade event streaming via WebSocket
//   - Comprehensive input validation using struct tags and validator
//   - Financial precision using decimal.Decimal for price/quantity data
//   - Symbol normalization between Binance format and standard format
//   - Robust error handling with structured logging
//   - Configurable connection parameters
package exchange

import (
	"candles/internal/model"
	"candles/internal/utils"
	"candles/internal/websocket"
	"context"

	json "github.com/goccy/go-json"
	"github.com/rs/zerolog/log"
	"github.com/shopspring/decimal"

	"fmt"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
)

var (
	// defaultBinanceConfig provides sensible default configuration values for Binance connections.
	defaultBinanceConfig = ExchangeConfig{
		BaseURL:    "wss://stream.binance.com:9443",
		MaxSymbols: 10,
	}
)

// BinanceConnector provides a Binance-specific implementation of the ExchangeConnector interface.
//
// It manages WebSocket connections to Binance's real-time trade data streams,
// handling the exchange-specific message format and converting raw trade data
// into standardized TradeEvent structures.
type BinanceConnector struct {
	config   ExchangeConfig      // Configuration parameters for the connector
	validate *validator.Validate // Validator instance for message validation
}

// msg represents the outer wrapper structure for Binance WebSocket messages.
//
// Binance uses a nested JSON format where trade data is embedded within
// a stream wrapper that identifies the specific data stream and contains
// the actual trade payload as raw JSON.
//
// Example Binance message format:
//
//	{
//		"stream": "btcusdt@trade",
//		"data": {
//			"s": "BTCUSDT",
//			"p": "50000.12",
//			"q": "0.001",
//			"T": 1634567890123
//		}
//	}
type msg struct {
	Stream string          `json:"stream" validate:"required"` // Stream identifier (e.g., "btcusdt@trade")
	Data   json.RawMessage `json:"data" validate:"required"`   // Raw JSON payload containing trade data
}

// trade represents the inner trade data structure from Binance WebSocket messages.
//
// This structure maps directly to Binance's trade event format, using string
// representations for numeric values to preserve precision during JSON parsing.
// The Time field represents the trade execution time in Unix milliseconds.
//
// Field validation ensures:
//   - Symbol is present and non-empty
//   - Price and Quantity are numeric strings
//   - Time is a positive Unix timestamp in milliseconds
type trade struct {
	Symbol   string `json:"s" validate:"required"`         // Trading pair symbol (e.g., "BTCUSDT")
	Price    string `json:"p" validate:"required,numeric"` // Trade execution price as string
	Quantity string `json:"q" validate:"required,numeric"` // Trade quantity as string
	Time     int64  `json:"T" validate:"required,gt=0"`    // Trade time in Unix milliseconds
}

// NewBinanceConnector creates a new Binance connector with the specified configuration.
//
// If no configuration is provided (cfg is nil), the connector will use default
// configuration values suitable for most use cases. The configuration is validated
// against the defaults to ensure all required fields are present and valid.
func NewBinanceConnector(cfg *ExchangeConfig) (*BinanceConnector, error) {
	if cfg == nil {
		cfg = &defaultBinanceConfig
	}

	if err := validateConfig(cfg, &defaultBinanceConfig); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	return &BinanceConnector{
		config:   *cfg,
		validate: validator.New(),
	}, nil
}

// SubscribeToTrades establishes a WebSocket connection to Binance and subscribes to
// trade events for the specified trading pairs.
//
// This method performs the following operations:
//  1. Validates the provided trading pairs against exchange limits
//  2. Constructs the appropriate Binance WebSocket stream URL
//  3. Creates a WebSocket client with the configured message handler
//  4. Returns a channel that will receive parsed trade events
//
// The returned channel will receive TradeEvent structures containing:
//   - Normalized symbol format (e.g., "BTC-USDT")
//   - Precise decimal values for price and quantity
//   - Exchange and local timestamps
//   - Exchange identifier (BinanceExchange)
func (bc *BinanceConnector) SubscribeToTrades(ctx context.Context, pairs []string) (<-chan model.TradeEvent, error) {
	if err := utils.ValidatePairs(pairs, bc.config.MaxSymbols); err != nil {
		return nil, err
	}

	streamURL, err := bc.buildStreamUrl(pairs)
	if err != nil {
		return nil, err
	}

	client, err := websocket.NewWebsocketClient(ctx, websocket.Config{
		Endpoint: streamURL,
		Handler:  bc.handleTradeMessage,
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to create Binance WebSocket client")
		return nil, err
	}

	return client.TradeChan, nil
}

// buildStreamUrl constructs the WebSocket URL for subscribing to multiple trade streams.
//
// Binance WebSocket API supports combined streams using the format:
// wss://stream.binance.com:9443/stream?streams=symbol1@trade/symbol2@trade/...
func (bc *BinanceConnector) buildStreamUrl(pairs []string) (string, error) {
	streams := make([]string, 0, len(pairs))

	for _, s := range pairs {
		if err := utils.ValidateSymbol(s); err != nil {
			return "", err
		}
		streams = append(streams, fmt.Sprintf("%s@trade",
			strings.ToLower(strings.ReplaceAll(s, "-", ""))))
	}

	streamURL := fmt.Sprintf("%s/stream?streams=%s",
		bc.config.BaseURL, strings.Join(streams, "/"))

	return streamURL, nil
}

// handleTradeMessage processes raw WebSocket messages from Binance and converts them
// to standardized TradeEvent structures.
func (bc *BinanceConnector) handleTradeMessage(raw []byte, tradeChan chan<- model.TradeEvent) error {
	// decode the outer wrapper
	var m msg
	if err := json.Unmarshal(raw, &m); err != nil {
		log.Error().Err(err).Msg("invalid outer JSON")
		return err
	}

	// decode the trade payload
	var t trade
	if err := json.Unmarshal(m.Data, &t); err != nil {
		log.Error().Err(err).Msg("invalid trade payload JSON")
		return err
	}

	// validate in-place
	if err := bc.validate.Struct(&t); err != nil {
		log.Warn().Err(err).Interface("trade", t).Msg("trade validation failed")
		return err
	}

	price, err := decimal.NewFromString(t.Price)
	if err != nil {
		log.Error().Err(err).Msg("invalid trade price")
		return err
	}

	quantity, err := decimal.NewFromString(t.Quantity)
	if err != nil {
		log.Error().Err(err).Msg("invalid trade quantity")
		return err
	}

	// push to channel
	tradeChan <- model.TradeEvent{
		Pair:      toNormalizedSymbol(t.Symbol),
		Price:     price,
		Quantity:  quantity,
		Timestamp: time.UnixMilli(t.Time),
		Exchange:  model.BinanceExchange,
	}

	return nil
}

// toNormalizedSymbol converts Binance symbol format to standardized format.
func toNormalizedSymbol(symbol string) string {
	symbol = strings.ToLower(symbol)

	for quote := range utils.QuoteAssetSet {
		quote = strings.ToLower(quote)
		if strings.HasSuffix(symbol, quote) {
			base := symbol[:len(symbol)-len(quote)]
			return strings.ToUpper(base) + "-" + strings.ToUpper(quote)
		}
	}

	return strings.ToUpper(symbol)
}
