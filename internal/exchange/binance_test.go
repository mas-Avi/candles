package exchange

import (
	"candles/internal/model"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createValidConfig creates a valid test configuration
func createValidConfig() *ExchangeConfig {
	return &ExchangeConfig{
		BaseURL:    "wss://stream.binance.com:9443",
		MaxSymbols: 10,
	}
}

// createTestTradeMessage creates a realistic Binance trade message for testing
func createTestTradeMessage(symbol, price, quantity string, timestamp int64) []byte {
	tradeData := trade{
		Symbol:   symbol,
		Price:    price,
		Quantity: quantity,
		Time:     timestamp,
	}

	dataBytes, _ := json.Marshal(tradeData)

	message := msg{
		Stream: fmt.Sprintf("%s@trade", strings.ToLower(symbol)),
		Data:   dataBytes,
	}

	messageBytes, _ := json.Marshal(message)
	return messageBytes
}

// Test_NewBinanceConnector tests the connector constructor with various configurations
func Test_NewBinanceConnector(t *testing.T) {
	tests := []struct {
		name        string
		config      *ExchangeConfig
		expectError bool
		description string
	}{
		{
			name:        "Valid configuration",
			config:      createValidConfig(),
			expectError: false,
			description: "Should create connector with valid configuration",
		},
		{
			name:        "Nil configuration uses defaults",
			config:      nil,
			expectError: false,
			description: "Should use default configuration when nil is provided",
		},
		{
			name: "Custom configuration",
			config: &ExchangeConfig{
				BaseURL:    "wss://testnet.binance.vision",
				MaxSymbols: 5,
			},
			expectError: false,
			description: "Should accept custom configuration values",
		},
		{
			name: "Empty BaseURL",
			config: &ExchangeConfig{
				BaseURL:    "", // empty URL
				MaxSymbols: 10,
			},
			expectError: false,
			description: "Should use default BaseURL",
		},
		{
			name: "Invalid MaxSymbols",
			config: &ExchangeConfig{
				BaseURL:    "wss://stream.binance.com:9443",
				MaxSymbols: 0, // Invalid zero limit
			},
			expectError: false,
			description: "Should use default MaxSymbols",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			connector, err := NewBinanceConnector(tt.config)

			if tt.expectError {
				assert.Error(t, err, tt.description)
				assert.Nil(t, connector, "Should not return connector on error")
			} else {
				assert.NoError(t, err, tt.description)
				assert.NotNil(t, connector, "Should return valid connector")

				if connector != nil {
					assert.NotNil(t, connector.validate, "Should have validator")

					// Verify configuration is stored
					if tt.config != nil {
						assert.Equal(t, tt.config.BaseURL, connector.config.BaseURL, "Should store BaseURL")
						assert.Equal(t, tt.config.MaxSymbols, connector.config.MaxSymbols, "Should store MaxSymbols")
					} else {
						// Should use defaults
						assert.Equal(t, defaultBinanceConfig.BaseURL, connector.config.BaseURL, "Should use default BaseURL")
						assert.Equal(t, defaultBinanceConfig.MaxSymbols, connector.config.MaxSymbols, "Should use default MaxSymbols")
					}
				}
			}
		})
	}
}

// Test_buildStreamUrl tests WebSocket URL construction for various symbol combinations
func Test_buildStreamUrl(t *testing.T) {
	connector, err := NewBinanceConnector(createValidConfig())
	require.NoError(t, err)

	tests := []struct {
		name        string
		pairs       []string
		expectedURL string
		expectError bool
		description string
	}{
		{
			name:        "Single trading pair",
			pairs:       []string{"BTC-USDT"},
			expectedURL: "wss://stream.binance.com:9443/stream?streams=btcusdt@trade",
			expectError: false,
			description: "Should build URL for single pair",
		},
		{
			name:        "Multiple trading pairs",
			pairs:       []string{"BTC-USDT", "ETH-USDT", "SOL-USDT"},
			expectedURL: "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade",
			expectError: false,
			description: "Should build URL for multiple pairs",
		},
		{
			name:        "Mixed case symbols",
			pairs:       []string{"btc-usdt", "ETH-USDT", "SOL-Usdt"},
			expectedURL: "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade",
			expectError: false,
			description: "Should normalize case for all symbols",
		},
		{
			name:        "Different quote assets",
			pairs:       []string{"BTC-USDT", "ETH-BTC", "SOL-ETH"},
			expectedURL: "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethbtc@trade/soleth@trade",
			expectError: false,
			description: "Should handle different quote assets",
		},
		{
			name:        "Empty pairs list",
			pairs:       []string{},
			expectedURL: "wss://stream.binance.com:9443/stream?streams=",
			expectError: false,
			description: "Should handle empty pairs list",
		},
		{
			name:        "Invalid symbol format",
			pairs:       []string{"INVALID"},
			expectedURL: "",
			expectError: true,
			description: "Should reject invalid symbol format",
		},
		{
			name:        "Symbol without hyphen",
			pairs:       []string{"BTCUSDT"},
			expectedURL: "",
			expectError: true,
			description: "Should reject symbol without hyphen separator",
		},
		{
			name:        "Empty symbol",
			pairs:       []string{""},
			expectedURL: "",
			expectError: true,
			description: "Should reject empty symbol",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, err := connector.buildStreamUrl(tt.pairs)

			if tt.expectError {
				assert.Error(t, err, tt.description)
				assert.Empty(t, url, "Should return empty URL on error")
			} else {
				assert.NoError(t, err, tt.description)
				assert.Equal(t, tt.expectedURL, url, tt.description)
			}
		})
	}
}

// TestBinance_handleTradeMessage tests trade message parsing and validation
func TestBinance_handleTradeMessage(t *testing.T) {
	connector, err := NewBinanceConnector(createValidConfig())
	require.NoError(t, err)

	// Create channel for receiving trade events
	tradeChan := make(chan model.TradeEvent, 10)

	tests := []struct {
		name          string
		message       []byte
		expectError   bool
		expectedTrade *model.TradeEvent
		description   string
	}{
		{
			name:        "Valid trade message",
			message:     createTestTradeMessage("BTCUSDT", "50000.12345678", "0.001", 1640995200000),
			expectError: false,
			expectedTrade: &model.TradeEvent{
				Pair:      "BTC-USDT",
				Price:     decimal.RequireFromString("50000.12345678"),
				Quantity:  decimal.RequireFromString("0.001"),
				Timestamp: time.UnixMilli(1640995200000),
				Exchange:  model.BinanceExchange,
			},
			description: "Should parse valid trade message correctly",
		},
		{
			name:        "High precision values",
			message:     createTestTradeMessage("ETHUSDT", "3000.123456789", "0.000000001", 1640995300000),
			expectError: false,
			expectedTrade: &model.TradeEvent{
				Pair:      "ETH-USDT",
				Price:     decimal.RequireFromString("3000.123456789"),
				Quantity:  decimal.RequireFromString("0.000000001"),
				Timestamp: time.UnixMilli(1640995300000),
				Exchange:  model.BinanceExchange,
			},
			description: "Should handle high precision decimal values",
		},
		{
			name:        "Large values",
			message:     createTestTradeMessage("BTCUSDT", "100000.00", "1000.123456", 1640995400000),
			expectError: false,
			expectedTrade: &model.TradeEvent{
				Pair:      "BTC-USDT",
				Price:     decimal.RequireFromString("100000.00"),
				Quantity:  decimal.RequireFromString("1000.123456"),
				Timestamp: time.UnixMilli(1640995400000),
				Exchange:  model.BinanceExchange,
			},
			description: "Should handle large price and quantity values",
		},
		{
			name:        "Different quote asset",
			message:     createTestTradeMessage("ETHBTC", "0.075", "10.5", 1640995500000),
			expectError: false,
			expectedTrade: &model.TradeEvent{
				Pair:      "ETH-BTC",
				Price:     decimal.RequireFromString("0.075"),
				Quantity:  decimal.RequireFromString("10.5"),
				Timestamp: time.UnixMilli(1640995500000),
				Exchange:  model.BinanceExchange,
			},
			description: "Should handle different quote assets correctly",
		},
		{
			name:        "Invalid JSON",
			message:     []byte(`{"invalid": json}`),
			expectError: true,
			description: "Should reject invalid JSON",
		},
		{
			name:        "Missing data field",
			message:     []byte(`{"stream": "btcusdt@trade"}`),
			expectError: true,
			description: "Should reject message without data field",
		},
		{
			name: "Invalid trade data structure",
			message: func() []byte {
				invalidData := `{"invalid": "data"}`
				wrapper := fmt.Sprintf(`{"stream": "btcusdt@trade", "data": %s}`, invalidData)
				return []byte(wrapper)
			}(),
			expectError: true,
			description: "Should reject invalid trade data structure",
		},
		{
			name:        "Missing symbol",
			message:     createTestTradeMessageWithMissingField("s"),
			expectError: true,
			description: "Should reject message without symbol",
		},
		{
			name:        "Missing price",
			message:     createTestTradeMessageWithMissingField("p"),
			expectError: true,
			description: "Should reject message without price",
		},
		{
			name:        "Missing quantity",
			message:     createTestTradeMessageWithMissingField("q"),
			expectError: true,
			description: "Should reject message without quantity",
		},
		{
			name:        "Missing timestamp",
			message:     createTestTradeMessageWithMissingField("T"),
			expectError: true,
			description: "Should reject message without timestamp",
		},
		{
			name:        "Invalid price format",
			message:     createTestTradeMessage("BTCUSDT", "invalid", "0.001", 1640995200000),
			expectError: true,
			description: "Should reject invalid price format",
		},
		{
			name:        "Invalid quantity format",
			message:     createTestTradeMessage("BTCUSDT", "50000", "invalid", 1640995200000),
			expectError: true,
			description: "Should reject invalid quantity format",
		},
		{
			name:        "Zero timestamp",
			message:     createTestTradeMessage("BTCUSDT", "50000", "0.001", 0),
			expectError: true,
			description: "Should reject zero timestamp",
		},
		{
			name:        "Negative timestamp",
			message:     createTestTradeMessage("BTCUSDT", "50000", "0.001", -1),
			expectError: true,
			description: "Should reject negative timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := connector.handleTradeMessage(tt.message, tradeChan)

			if tt.expectError {
				assert.Error(t, err, tt.description)

				// Ensure no trade was sent to channel
				select {
				case trade := <-tradeChan:
					t.Errorf("Should not receive trade on error, but got: %+v", trade)
				default:
					// Expected - no trade should be sent
				}
			} else {
				assert.NoError(t, err, tt.description)

				// Verify trade was sent to channel
				select {
				case trade := <-tradeChan:
					assert.Equal(t, tt.expectedTrade.Pair, trade.Pair, "Should have correct pair")
					assert.True(t, tt.expectedTrade.Price.Equal(trade.Price), "Should have correct price")
					assert.True(t, tt.expectedTrade.Quantity.Equal(trade.Quantity), "Should have correct quantity")
					assert.Equal(t, tt.expectedTrade.Timestamp, trade.Timestamp, "Should have correct timestamp")
					assert.Equal(t, tt.expectedTrade.Exchange, trade.Exchange, "Should have correct exchange")

				case <-time.After(100 * time.Millisecond):
					t.Error("Should receive trade event within timeout")
				}
			}
		})
	}
}

// createTestTradeMessageWithMissingField creates a test message with a specific field missing
func createTestTradeMessageWithMissingField(missingField string) []byte {
	tradeMap := map[string]interface{}{
		"s": "BTCUSDT",
		"p": "50000",
		"q": "0.001",
		"T": int64(1640995200000),
	}

	// Remove the specified field
	delete(tradeMap, missingField)

	dataBytes, _ := json.Marshal(tradeMap)

	message := msg{
		Stream: "btcusdt@trade",
		Data:   dataBytes,
	}

	messageBytes, _ := json.Marshal(message)
	return messageBytes
}

// Test_toNormalizedSymbol tests symbol format conversion from Binance to standard format
func Test_toNormalizedSymbol(t *testing.T) {
	tests := []struct {
		name           string
		binanceSymbol  string
		expectedSymbol string
		description    string
	}{
		{
			name:           "BTC-USDT pair",
			binanceSymbol:  "BTCUSDT",
			expectedSymbol: "BTC-USDT",
			description:    "Should convert BTCUSDT to BTC-USDT",
		},
		{
			name:           "ETH-USDT pair",
			binanceSymbol:  "ETHUSDT",
			expectedSymbol: "ETH-USDT",
			description:    "Should convert ETHUSDT to ETH-USDT",
		},
		{
			name:           "ETH-BTC pair",
			binanceSymbol:  "ETHBTC",
			expectedSymbol: "ETH-BTC",
			description:    "Should convert ETHBTC to ETH-BTC",
		},
		{
			name:           "BNB-ETH pair",
			binanceSymbol:  "BNBETH",
			expectedSymbol: "BNB-ETH",
			description:    "Should convert BNBETH to BNB-ETH",
		},
		{
			name:           "ADA-USDT pair",
			binanceSymbol:  "ADAUSDT",
			expectedSymbol: "ADA-USDT",
			description:    "Should convert ADAUSDT to ADA-USDT",
		},
		{
			name:           "Lowercase input",
			binanceSymbol:  "btcusdt",
			expectedSymbol: "BTC-USDT",
			description:    "Should handle lowercase input",
		},
		{
			name:           "Mixed case input",
			binanceSymbol:  "BtcUsDt",
			expectedSymbol: "BTC-USDT",
			description:    "Should handle mixed case input",
		},
		{
			name:           "Long symbol with USDT",
			binanceSymbol:  "SHIBACOINDOGE",
			expectedSymbol: "SHIBACOINDOGE", // No known quote asset match
			description:    "Should return unchanged if no quote asset match",
		},
		{
			name:           "BTC quote asset",
			binanceSymbol:  "ETHBTC",
			expectedSymbol: "ETH-BTC",
			description:    "Should handle BTC as quote asset",
		},
		{
			name:           "Unknown quote asset",
			binanceSymbol:  "BTCXYZ",
			expectedSymbol: "BTCXYZ",
			description:    "Should return uppercase if no quote asset match",
		},
		{
			name:           "Empty string",
			binanceSymbol:  "",
			expectedSymbol: "",
			description:    "Should handle empty string",
		},
		{
			name:           "Single character",
			binanceSymbol:  "A",
			expectedSymbol: "A",
			description:    "Should handle single character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toNormalizedSymbol(tt.binanceSymbol)
			assert.Equal(t, tt.expectedSymbol, result, tt.description)
		})
	}
}

// Test_HandleTradeMessage_Concurrency tests concurrent message handling
func Test_HandleTradeMessage_Concurrency(t *testing.T) {
	connector, err := NewBinanceConnector(createValidConfig())
	require.NoError(t, err)

	tradeChan := make(chan model.TradeEvent, 1000)
	numMessages := 100
	numWorkers := 10

	// Create test messages
	messages := make([][]byte, numMessages)
	for i := 0; i < numMessages; i++ {
		messages[i] = createTestTradeMessage("BTCUSDT", fmt.Sprintf("50%03d", i), "0.001", int64(1640995200000+i))
	}

	// Process messages concurrently
	done := make(chan bool, numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer func() { done <- true }()

			start := workerID * (numMessages / numWorkers)
			end := start + (numMessages / numWorkers)
			if workerID == numWorkers-1 {
				end = numMessages // Handle remainder for last worker
			}

			for i := start; i < end; i++ {
				err := connector.handleTradeMessage(messages[i], tradeChan)
				assert.NoError(t, err, "Should handle message %d without error", i)
			}
		}(w)
	}

	// Wait for all workers to complete
	for w := 0; w < numWorkers; w++ {
		<-done
	}

	// Verify all trades were received
	close(tradeChan)
	receivedCount := 0
	for range tradeChan {
		receivedCount++
	}

	assert.Equal(t, numMessages, receivedCount, "Should receive all processed trades")
}

// TestBinance_Error_Handling tests various error scenarios
func TestBinance_Error_Handling(t *testing.T) {
	connector, err := NewBinanceConnector(createValidConfig())
	require.NoError(t, err)

	tradeChan := make(chan model.TradeEvent, 10)

	tests := []struct {
		name        string
		message     []byte
		description string
	}{
		{
			name:        "Malformed JSON",
			message:     []byte(`{malformed json`),
			description: "Should handle malformed JSON gracefully",
		},
		{
			name:        "Empty message",
			message:     []byte(``),
			description: "Should handle empty message gracefully",
		},
		{
			name:        "Null message",
			message:     []byte(`null`),
			description: "Should handle null JSON gracefully",
		},
		{
			name:        "Array instead of object",
			message:     []byte(`[]`),
			description: "Should handle array JSON gracefully",
		},
		{
			name:        "String instead of object",
			message:     []byte(`"string"`),
			description: "Should handle string JSON gracefully",
		},
		{
			name:        "Number instead of object",
			message:     []byte(`123`),
			description: "Should handle number JSON gracefully",
		},
		{
			name:        "Boolean instead of object",
			message:     []byte(`true`),
			description: "Should handle boolean JSON gracefully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := connector.handleTradeMessage(tt.message, tradeChan)
			assert.Error(t, err, tt.description)

			// Ensure no trade was sent to channel
			select {
			case trade := <-tradeChan:
				t.Errorf("Should not receive trade on error, but got: %+v", trade)
			default:
				// Expected - no trade should be sent
			}
		})
	}
}

// Test_Symbol_Normalization_Edge_Cases tests edge cases in symbol normalization
func Test_Symbol_Normalization_Edge_Cases(t *testing.T) {
	tests := []struct {
		name           string
		binanceSymbol  string
		expectedSymbol string
		description    string
	}{
		{
			name:           "Very long base asset",
			binanceSymbol:  "VERYLONGBASEASSETUSDT",
			expectedSymbol: "VERYLONGBASEASSET-USDT",
			description:    "Should handle long base asset names",
		},
		{
			name:           "Single character base",
			binanceSymbol:  "AUSDT",
			expectedSymbol: "A-USDT",
			description:    "Should handle single character base asset",
		},
		{
			name:           "Numeric in symbol",
			binanceSymbol:  "BTC2USDT",
			expectedSymbol: "BTC2-USDT",
			description:    "Should handle numbers in symbols",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := toNormalizedSymbol(tt.binanceSymbol)
			assert.Equal(t, tt.expectedSymbol, result, tt.description)
		})
	}
}

// Benchmark_Binance_handleTradeMessage benchmarks message handling performance
func Benchmark_Binance_handleTradeMessage(b *testing.B) {
	connector, err := NewBinanceConnector(createValidConfig())
	require.NoError(b, err)

	tradeChan := make(chan model.TradeEvent, 1000)
	message := createTestTradeMessage("BTCUSDT", "50000.12345678", "0.001", 1640995200000)

	// Consume trades to prevent channel blocking
	go func() {
		for range tradeChan {
			// Discard trades
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := connector.handleTradeMessage(message, tradeChan)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark_toNormalizedSymbol benchmarks symbol normalization performance
func Benchmark_toNormalizedSymbol(b *testing.B) {
	symbols := []string{"BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "LINKUSDT"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		symbol := symbols[i%len(symbols)]
		_ = toNormalizedSymbol(symbol)
	}
}

// Benchmark_buildStreamUrl benchmarks URL construction performance
func Benchmark_buildStreamUrl(b *testing.B) {
	connector, err := NewBinanceConnector(createValidConfig())
	require.NoError(b, err)

	pairs := []string{"BTC-USDT", "ETH-USDT", "ADA-USDT", "DOT-USDT", "LINK-USDT"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := connector.buildStreamUrl(pairs)
		if err != nil {
			b.Fatal(err)
		}
	}
}
