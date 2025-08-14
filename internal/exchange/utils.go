// Package exchange provides cryptocurrency exchange connectors for real-time trading data.
//
// This file contains shared utilities, configuration structures, and validation functions
// used across all exchange connector implementations. It provides a common foundation
// for configuration management and error handling.
package exchange

import (
	"errors"
)

var (
	// ErrInvalidConfig indicates that the provided ExchangeConfig contains invalid values.
	ErrInvalidConfig = errors.New("invalid configuration")
)

// ExchangeConfig provides common configuration parameters for all exchange connectors.
//
// This structure standardizes configuration across different cryptocurrency exchanges,
// ensuring consistent behavior and easy management of connection parameters.
type ExchangeConfig struct {
	// BaseURL is the WebSocket endpoint URL for the exchange API.
	BaseURL string

	// MaxSymbols is the maximum number of trading pairs that can be subscribed to simultaneously.
	MaxSymbols int
}

// validateConfig ensures all required configuration fields are present and valid,
// applying sensible defaults for optional fields when possible.
func validateConfig(cfg *ExchangeConfig, defaultCfg *ExchangeConfig) error {

	// Apply defaults for optional fields
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultCfg.BaseURL
	}

	if cfg.MaxSymbols <= 0 {
		cfg.MaxSymbols = defaultCfg.MaxSymbols
	}

	// All validations passed
	return nil
}
