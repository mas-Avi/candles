// Package utils provides common utility functions for data validation.
//
// This package contains utilities for working with cryptocurrency trading data,
// including validating trading pair symbols according to supported quote assets.
package utils

import (
	"errors"
	"fmt"
	"strings"
)

// Error definitions for validation functions
var (
	ErrNoSymbols      = errors.New("zero symbols requested")
	ErrTooManySymbols = errors.New("too many symbols requested")
)

// quoteAssetSet contains the supported quote assets for trading pairs.
// This map is used for O(1) lookup performance when validating symbols.
var QuoteAssetSet = map[string]bool{
	"USDT": true, // Tether USD
	"BTC":  true, // Bitcoin
	"ETH":  true, // Ethereum
	"SOL":  true, // Solana
}

//TODO extract USDT out

// supportedQuotesCache is a pre-computed string of supported quote assets
// to avoid rebuilding this string on every validation error.
var supportedQuotesCache = getSupportedQuotes(QuoteAssetSet)

// ValidateSymbol validates that a trading pair symbol follows the expected format
// and uses a supported quote asset.
//
// The expected format is "BASE-QUOTE" where:
//   - BASE is the base asset (e.g., "BTC", "ETH")
//   - QUOTE is the quote asset and must be one of the supported quote assets
//
// Supported quote assets: USDT, BTC, ETH, SOL
//
// The validation is case-insensitive for the quote asset.
func ValidateSymbol(symbol string) error {
	if symbol == "" {
		return errors.New("symbol cannot be empty")
	}

	parts := strings.Split(symbol, "-")
	if len(parts) != 2 {
		return fmt.Errorf("invalid symbol format: expected BASE-QUOTE, got %q", symbol)
	}

	if len(parts[0]) == 0 {
		return errors.New("base asset cannot be empty")
	}

	if len(parts[1]) == 0 {
		return errors.New("quote asset cannot be empty")
	}

	base := strings.ToUpper(parts[0])
	if !QuoteAssetSet[base] {
		return fmt.Errorf("unsupported base asset: %s (supported: %s)",
			base, supportedQuotesCache)
	}

	quote := strings.ToUpper(parts[1])
	if !QuoteAssetSet[quote] {
		return fmt.Errorf("unsupported quote asset: %s (supported: %s)",
			quote, supportedQuotesCache)
	}

	return nil
}

// ValidatePairs validates a slice of trading pair symbols and enforces quantity limits.
//
// This function performs two types of validation:
//  1. Quantity validation: Ensures the number of pairs is within acceptable limits
//  2. Format validation: Validates each symbol using ValidateSymbol
func ValidatePairs(pairs []string, maxAllowed int) error {
	if len(pairs) == 0 {
		return ErrNoSymbols
	}

	if maxAllowed <= 0 {
		return fmt.Errorf("%w: max allowed must be positive, got %d",
			ErrTooManySymbols, maxAllowed)
	}

	if len(pairs) > maxAllowed {
		return fmt.Errorf("%w: requested %d symbols, maximum allowed %d",
			ErrTooManySymbols, len(pairs), maxAllowed)
	}

	for i, symbol := range pairs {
		if err := ValidateSymbol(symbol); err != nil {
			return fmt.Errorf("invalid symbol at index %d (%q): %w", i, symbol, err)
		}
	}

	return nil
}

// getSupportedQuotes builds a comma-separated string of supported quote assets
// from the provided quote asset set. This function is used to generate
// user-friendly error messages.
//
// Note: The order of assets in the returned string is not guaranteed due to
// Go's map iteration order being unspecified.
func getSupportedQuotes(quoteAssetSet map[string]bool) string {
	keys := make([]string, 0, len(quoteAssetSet))
	for k := range quoteAssetSet {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}
