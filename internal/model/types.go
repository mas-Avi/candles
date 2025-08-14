// Package model defines core data types for the candle streaming service.
//
// This package contains fundamental data structures used throughout the system
// for representing trading events, market data, and exchange information.
// All types use decimal.Decimal for precise financial calculations to avoid
// floating-point precision issues common in financial applications.
package model

import (
	"time"

	"github.com/shopspring/decimal"
)

// Exchange represents a cryptocurrency exchange.
type Exchange int

const (
	// BinanceExchange represents the Binance cryptocurrency exchange
	BinanceExchange Exchange = iota

	// CoinbaseExchange represents the Coinbase Pro cryptocurrency exchange
	CoinbaseExchange

	// OkxExchange represents the OKX cryptocurrency exchange
	OkxExchange
)

// TradeEvent represents a simplified trade message from a cryptocurrency exchange.
//
// This structure contains all essential information about a trade execution,
// including pricing, volume, timing, and source exchange. It serves as the
// primary input for candle aggregation processes.
//
// All monetary values use decimal.Decimal to ensure precise financial calculations
// without floating-point rounding errors that could accumulate over time.
type TradeEvent struct {
	Pair      string          // Trading pair symbol (e.g., "BTC-USDT")
	Price     decimal.Decimal // Trade execution price (precise decimal)
	Quantity  decimal.Decimal // Volume of base asset traded (precise decimal)
	Timestamp time.Time       // Exchange timestamp of the trade
	Exchange  Exchange        // Source exchange
}

// OHLCCandle represents a time-based aggregated candle with OHLC (Open, High, Low, Close) data.
//
// This structure contains aggregated trading data over a specific time period,
// commonly used in financial charting and technical analysis. Each candle
// represents market activity within its time window.
//
// All price and volume fields use decimal.Decimal to maintain precision
// throughout aggregation calculations and prevent rounding errors.
//
// Fields:
//   - Pair: Trading pair symbol this candle represents
//   - Open: Opening price at the start of the time period
//   - High: Highest price during the time period
//   - Low: Lowest price during the time period
//   - Close: Closing price at the end of the time period
//   - Volume: Total volume traded during the time period
//   - StartTime: Beginning of the candle's time window
//   - EndTime: End of the candle's time window
type OHLCCandle struct {
	Pair      string          // Trading pair symbol (e.g., "BTC-USDT")
	Open      decimal.Decimal // Opening price (precise decimal)
	High      decimal.Decimal // Highest price in period (precise decimal)
	Low       decimal.Decimal // Lowest price in period (precise decimal)
	Close     decimal.Decimal // Closing price (precise decimal)
	Volume    decimal.Decimal // Total volume traded (precise decimal)
	StartTime time.Time       // Start of time window (inclusive)
	EndTime   time.Time       // End of time window (inclusive)
}
