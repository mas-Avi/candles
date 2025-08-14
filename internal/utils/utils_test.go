package utils

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test_ValidateSymbol tests the ValidateSymbol function with various inputs
func Test_ValidateSymbol(t *testing.T) {
	tests := []struct {
		name        string
		symbol      string
		expectError bool
		errorMsg    string
		description string
	}{
		// Valid cases
		{
			name:        "Valid BTC-USDT",
			symbol:      "BTC-USDT",
			expectError: false,
			description: "Should accept valid BTC-USDT symbol",
		},
		{
			name:        "Valid ETH-BTC",
			symbol:      "ETH-BTC",
			expectError: false,
			description: "Should accept valid ETH-BTC symbol",
		},
		{
			name:        "Case insensitive quote asset - lowercase",
			symbol:      "BTC-usdt",
			expectError: false,
			description: "Should accept lowercase quote asset",
		},
		{
			name:        "Case insensitive quote asset - mixed case",
			symbol:      "ETH-Btc",
			expectError: false,
			description: "Should accept mixed case quote asset",
		},

		// Invalid cases - empty symbol
		{
			name:        "Empty symbol",
			symbol:      "",
			expectError: true,
			errorMsg:    "symbol cannot be empty",
			description: "Should reject empty symbol",
		},

		// Invalid cases - format errors
		{
			name:        "Missing hyphen",
			symbol:      "BTCUSDT",
			expectError: true,
			errorMsg:    "invalid symbol format",
			description: "Should reject symbol without hyphen",
		},
		{
			name:        "Multiple hyphens",
			symbol:      "BTC-USD-T",
			expectError: true,
			errorMsg:    "invalid symbol format",
			description: "Should reject symbol with multiple hyphens",
		},
		{
			name:        "Only hyphen",
			symbol:      "-",
			expectError: true,
			errorMsg:    "base asset cannot be empty",
			description: "Should reject symbol with only hyphen",
		},
		{
			name:        "Hyphen at start",
			symbol:      "-USDT",
			expectError: true,
			errorMsg:    "base asset cannot be empty",
			description: "Should reject symbol starting with hyphen",
		},
		{
			name:        "Hyphen at end",
			symbol:      "BTC-",
			expectError: true,
			errorMsg:    "quote asset cannot be empty",
			description: "Should reject symbol ending with hyphen",
		},
		{
			name:        "Multiple consecutive hyphens",
			symbol:      "BTC--USDT",
			expectError: true,
			errorMsg:    "invalid symbol format",
			description: "Should reject symbol with consecutive hyphens",
		},

		// Invalid cases - empty components
		{
			name:        "Empty base asset",
			symbol:      "-USDT",
			expectError: true,
			errorMsg:    "base asset cannot be empty",
			description: "Should reject empty base asset",
		},
		{
			name:        "Empty quote asset",
			symbol:      "BTC-",
			expectError: true,
			errorMsg:    "quote asset cannot be empty",
			description: "Should reject empty quote asset",
		},

		// Invalid cases - unsupported quote assets
		{
			name:        "Unsupported quote asset USD",
			symbol:      "BTC-USD",
			expectError: true,
			errorMsg:    "unsupported quote asset: USD",
			description: "Should reject unsupported USD quote asset",
		},
		{
			name:        "Unsupported quote asset EUR",
			symbol:      "ETH-EUR",
			expectError: true,
			errorMsg:    "unsupported quote asset: EUR",
			description: "Should reject unsupported EUR quote asset",
		},
		{
			name:        "Unsupported quote asset GBP",
			symbol:      "SOL-GBP",
			expectError: true,
			errorMsg:    "unsupported quote asset: GBP",
			description: "Should reject unsupported GBP quote asset",
		},
		{
			name:        "Random unsupported quote asset",
			symbol:      "BTC-RANDOM",
			expectError: true,
			errorMsg:    "unsupported quote asset: RANDOM",
			description: "Should reject random unsupported quote asset",
		},

		// Edge cases
		{
			name:        "Whitespace in symbol",
			symbol:      "BTC - USDT",
			expectError: true,
			errorMsg:    "unsupported base asset",
			description: "Should reject symbol with whitespace",
		},
		{
			name:        "Leading whitespace",
			symbol:      " BTC-USDT",
			expectError: true,
			errorMsg:    "unsupported base asset",
			description: "Should reject symbol with leading whitespace",
		},
		{
			name:        "Trailing whitespace",
			symbol:      "BTC-USDT ",
			expectError: true,
			errorMsg:    "unsupported quote asset",
			description: "Should reject symbol with trailing whitespace",
		},
		{
			name:        "Tab character",
			symbol:      "BTC\t-USDT",
			expectError: true,
			errorMsg:    "unsupported base asset",
			description: "Should reject symbol with tab character",
		},
		{
			name:        "Newline character",
			symbol:      "BTC-USDT\n",
			expectError: true,
			errorMsg:    "unsupported quote asset",
			description: "Should reject symbol with newline character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSymbol(tt.symbol)

			if tt.expectError {
				assert.Error(t, err, tt.description)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg, "Error message should contain expected text")
				}
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

// Test_ValidateSymbol_AllSupportedQuoteAssets tests all supported quote assets
func Test_ValidateSymbol_AllSupportedQuoteAssets(t *testing.T) {
	// Test all quote assets defined in QuoteAssetSet
	for quoteAsset := range QuoteAssetSet {
		t.Run("Quote asset "+quoteAsset, func(t *testing.T) {
			symbol := "BTC-" + quoteAsset
			err := ValidateSymbol(symbol)
			assert.NoError(t, err, "Should accept supported quote asset: %s", quoteAsset)

			// Test lowercase version
			symbolLower := "BTC-" + strings.ToLower(quoteAsset)
			err = ValidateSymbol(symbolLower)
			assert.NoError(t, err, "Should accept lowercase supported quote asset: %s", quoteAsset)
		})
	}
}

// Test_ValidatePairs tests the ValidatePairs function
func Test_ValidatePairs(t *testing.T) {
	tests := []struct {
		name        string
		pairs       []string
		maxAllowed  int
		expectError bool
		expectedErr error
		errorMsg    string
		description string
	}{
		// Valid cases
		{
			name:        "Valid single pair",
			pairs:       []string{"BTC-USDT"},
			maxAllowed:  1,
			expectError: false,
			description: "Should accept single valid pair",
		},
		{
			name:        "Valid multiple pairs",
			pairs:       []string{"BTC-USDT", "ETH-BTC", "SOL-ETH"},
			maxAllowed:  5,
			expectError: false,
			description: "Should accept multiple valid pairs",
		},
		{
			name:        "All supported quote assets",
			pairs:       []string{"BTC-USDT", "ETH-BTC", "USDT-ETH", "ETH-SOL"},
			maxAllowed:  10,
			expectError: false,
			description: "Should accept pairs with all supported quote assets",
		},
		{
			name:        "Maximum allowed pairs",
			pairs:       []string{"BTC-USDT", "ETH-USDT", "SOL-USDT"},
			maxAllowed:  3,
			expectError: false,
			description: "Should accept exactly maximum allowed pairs",
		},
		{
			name:        "Large maxAllowed",
			pairs:       []string{"BTC-USDT"},
			maxAllowed:  1000,
			expectError: false,
			description: "Should work with large maxAllowed value",
		},

		// Error cases - quantity validation
		{
			name:        "Empty pairs slice",
			pairs:       []string{},
			maxAllowed:  5,
			expectError: true,
			expectedErr: ErrNoSymbols,
			description: "Should reject empty pairs slice",
		},
		{
			name:        "Nil pairs slice",
			pairs:       nil,
			maxAllowed:  5,
			expectError: true,
			expectedErr: ErrNoSymbols,
			description: "Should reject nil pairs slice",
		},
		{
			name:        "Too many pairs",
			pairs:       []string{"BTC-USDT", "ETH-USDT", "ADA-USDT"},
			maxAllowed:  2,
			expectError: true,
			expectedErr: ErrTooManySymbols,
			errorMsg:    "requested 3 symbols, maximum allowed 2",
			description: "Should reject when pairs exceed maxAllowed",
		},
		{
			name:        "Zero maxAllowed",
			pairs:       []string{"BTC-USDT"},
			maxAllowed:  0,
			expectError: true,
			expectedErr: ErrTooManySymbols,
			errorMsg:    "max allowed must be positive, got 0",
			description: "Should reject zero maxAllowed",
		},
		{
			name:        "Negative maxAllowed",
			pairs:       []string{"BTC-USDT"},
			maxAllowed:  -1,
			expectError: true,
			expectedErr: ErrTooManySymbols,
			errorMsg:    "max allowed must be positive, got -1",
			description: "Should reject negative maxAllowed",
		},

		// Error cases - symbol validation
		{
			name:        "Invalid symbol format",
			pairs:       []string{"BTC-USDT", "INVALID"},
			maxAllowed:  5,
			expectError: true,
			errorMsg:    "invalid symbol at index 1",
			description: "Should reject slice with invalid symbol",
		},
		{
			name:        "Empty symbol in slice",
			pairs:       []string{"BTC-USDT", ""},
			maxAllowed:  5,
			expectError: true,
			errorMsg:    "invalid symbol at index 1",
			description: "Should reject slice with empty symbol",
		},
		{
			name:        "Unsupported quote asset",
			pairs:       []string{"BTC-USDT", "ETH-USD"},
			maxAllowed:  5,
			expectError: true,
			errorMsg:    "invalid symbol at index 1",
			description: "Should reject slice with unsupported quote asset",
		},
		{
			name:        "Mixed valid and invalid symbols",
			pairs:       []string{"BTC-USDT", "ETH-BTC", "INVALID", "ADA-ETH"},
			maxAllowed:  10,
			expectError: true,
			errorMsg:    "invalid symbol at index 2",
			description: "Should reject slice with any invalid symbol",
		},
		{
			name:        "Symbol without hyphen",
			pairs:       []string{"BTC-USDT", "ETHUSDT"},
			maxAllowed:  5,
			expectError: true,
			errorMsg:    "invalid symbol at index 1",
			description: "Should reject symbol without hyphen",
		},
		{
			name:        "Multiple hyphens in symbol",
			pairs:       []string{"BTC-USD-T"},
			maxAllowed:  5,
			expectError: true,
			errorMsg:    "invalid symbol at index 0",
			description: "Should reject symbol with multiple hyphens",
		},

		// Edge cases
		{
			name:        "Duplicate symbols",
			pairs:       []string{"BTC-USDT", "BTC-USDT"},
			maxAllowed:  5,
			expectError: false, // Function doesn't check for duplicates
			description: "Should not reject duplicate symbols (not its responsibility)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePairs(tt.pairs, tt.maxAllowed)

			if tt.expectError {
				assert.Error(t, err, tt.description)

				if tt.expectedErr != nil {
					assert.True(t, errors.Is(err, tt.expectedErr), "Should return expected error type")
				}

				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg, "Error message should contain expected text")
				}
			} else {
				assert.NoError(t, err, tt.description)
			}
		})
	}
}

// generateValidPairs generates a slice of valid trading pairs for testing
func generateValidPairs(count int) []string {
	pairs := make([]string, count)
	quoteAssets := []string{"USDT", "BTC", "ETH", "SOL"}

	for i := 0; i < count; i++ {
		baseAsset := "ASSET" + string(rune('A'+i%26)) // Generate unique base assets
		quoteAsset := quoteAssets[i%len(quoteAssets)]
		pairs[i] = baseAsset + "-" + quoteAsset
	}

	return pairs
}

// Test_getSupportedQuotes tests the getSupportedQuotes helper function
func Test_getSupportedQuotes(t *testing.T) {
	tests := []struct {
		name           string
		quoteAssetSet  map[string]bool
		expectedAssets []string
		description    string
	}{
		{
			name: "Standard quote asset set",
			quoteAssetSet: map[string]bool{
				"USDT": true,
				"BTC":  true,
				"ETH":  true,
			},
			expectedAssets: []string{"USDT", "BTC", "ETH"},
			description:    "Should return all quote assets from standard set",
		},
		{
			name:           "Empty quote asset set",
			quoteAssetSet:  map[string]bool{},
			expectedAssets: []string{},
			description:    "Should return empty string for empty set",
		},
		{
			name: "Single quote asset",
			quoteAssetSet: map[string]bool{
				"USDT": true,
			},
			expectedAssets: []string{"USDT"},
			description:    "Should return single quote asset",
		},
		{
			name: "Many quote assets",
			quoteAssetSet: map[string]bool{
				"USDT": true,
				"BTC":  true,
				"ETH":  true,
				"SOL":  true,
				"BNB":  true,
				"ADA":  true,
			},
			expectedAssets: []string{"USDT", "BTC", "ETH", "SOL", "BNB", "ADA"},
			description:    "Should return all quote assets from large set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getSupportedQuotes(tt.quoteAssetSet)

			if len(tt.expectedAssets) == 0 {
				assert.Equal(t, "", result, tt.description)
			} else {
				// Since map iteration order is not guaranteed, we need to check
				// that all expected assets are present in the result
				for _, asset := range tt.expectedAssets {
					assert.Contains(t, result, asset, "Result should contain asset: %s", asset)
				}

				// Check that the result contains exactly the expected number of assets
				parts := strings.Split(result, ", ")
				assert.Equal(t, len(tt.expectedAssets), len(parts), "Should have correct number of assets")
			}
		})
	}
}

// Test_QuoteAssetSet tests the package-level QuoteAssetSet variable
func Test_QuoteAssetSet(t *testing.T) {
	// Test that all expected quote assets are present
	expectedQuoteAssets := []string{"USDT", "BTC", "ETH", "SOL"}

	for _, asset := range expectedQuoteAssets {
		t.Run("Quote asset "+asset, func(t *testing.T) {
			exists := QuoteAssetSet[asset]
			assert.True(t, exists, "QuoteAssetSet should contain %s", asset)
		})
	}

	// Test that the set has the expected size
	assert.Equal(t, len(expectedQuoteAssets), len(QuoteAssetSet), "QuoteAssetSet should have expected number of assets")

	// Test that all values are true
	for asset, value := range QuoteAssetSet {
		assert.True(t, value, "All values in QuoteAssetSet should be true, but %s is %v", asset, value)
	}
}

// Test_supportedQuotesCache tests the package-level cache variable
func Test_supportedQuotesCache(t *testing.T) {
	// Test that cache is not empty
	assert.NotEmpty(t, supportedQuotesCache, "supportedQuotesCache should not be empty")

	// Test that cache contains all expected quote assets
	for asset := range QuoteAssetSet {
		assert.Contains(t, supportedQuotesCache, asset, "supportedQuotesCache should contain %s", asset)
	}

	// Test that cache matches what getSupportedQuotes would return
	expectedCache := getSupportedQuotes(QuoteAssetSet)

	// Since map iteration order is not guaranteed, we can't do direct string comparison
	// Instead, we'll verify that both contain the same assets
	cacheParts := strings.Split(supportedQuotesCache, ", ")
	expectedParts := strings.Split(expectedCache, ", ")

	assert.Equal(t, len(expectedParts), len(cacheParts), "Cache should have same number of assets as expected")

	for _, part := range cacheParts {
		assert.Contains(t, expectedCache, part, "Cache part should be in expected result")
	}
}

// Test_ErrorVariables tests the package-level error variables
func Test_ErrorVariables(t *testing.T) {
	t.Run("ErrNoSymbols", func(t *testing.T) {
		assert.NotNil(t, ErrNoSymbols, "ErrNoSymbols should not be nil")
		assert.Equal(t, "zero symbols requested", ErrNoSymbols.Error(), "ErrNoSymbols should have expected message")
	})

	t.Run("ErrTooManySymbols", func(t *testing.T) {
		assert.NotNil(t, ErrTooManySymbols, "ErrTooManySymbols should not be nil")
		assert.Equal(t, "too many symbols requested", ErrTooManySymbols.Error(), "ErrTooManySymbols should have expected message")
	})

	t.Run("Error unwrapping", func(t *testing.T) {
		// Test that errors can be properly identified with errors.Is()
		testErr := errors.New("test error")
		wrappedNoSymbols := errors.Join(ErrNoSymbols, testErr)
		wrappedTooMany := errors.Join(ErrTooManySymbols, testErr)

		assert.True(t, errors.Is(wrappedNoSymbols, ErrNoSymbols), "Should identify wrapped ErrNoSymbols")
		assert.True(t, errors.Is(wrappedTooMany, ErrTooManySymbols), "Should identify wrapped ErrTooManySymbols")
		assert.False(t, errors.Is(wrappedNoSymbols, ErrTooManySymbols), "Should not confuse error types")
	})
}

// Test_ValidatePairs_EdgeCases tests additional edge cases for ValidatePairs
func Test_ValidatePairs_EdgeCases(t *testing.T) {
	t.Run("Very large maxAllowed", func(t *testing.T) {
		pairs := []string{"BTC-USDT"}
		err := ValidatePairs(pairs, 1000000)
		assert.NoError(t, err, "Should handle very large maxAllowed")
	})

	t.Run("Maximum int maxAllowed", func(t *testing.T) {
		pairs := []string{"BTC-USDT"}
		err := ValidatePairs(pairs, int(^uint(0)>>1)) // Maximum int value
		assert.NoError(t, err, "Should handle maximum int maxAllowed")
	})

	t.Run("Error index accuracy", func(t *testing.T) {
		pairs := []string{
			"BTC-USDT", // index 0 - valid
			"ETH-BTC",  // index 1 - valid
			"INVALID",  // index 2 - invalid
			"ADA-ETH",  // index 3 - valid
		}

		err := ValidatePairs(pairs, 10)
		assert.Error(t, err, "Should reject slice with invalid symbol")
		assert.Contains(t, err.Error(), "index 2", "Error should specify correct index")
		assert.Contains(t, err.Error(), "INVALID", "Error should specify the invalid symbol")
	})
}

// Benchmark_ValidateSymbol benchmarks the ValidateSymbol function
func Benchmark_ValidateSymbol(b *testing.B) {
	symbols := []string{
		"BTC-USDT",
		"ETH-BTC",
		"ADA-ETH",
		"DOT-SOL",
		"INVALID",
		"BTC-USD", // Invalid quote asset
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		symbol := symbols[i%len(symbols)]
		ValidateSymbol(symbol)
	}
}

// Benchmark_ValidatePairs benchmarks the ValidatePairs function
func Benchmark_ValidatePairs(b *testing.B) {
	pairs := []string{
		"BTC-USDT", "ETH-BTC", "ADA-ETH", "DOT-SOL",
		"LINK-USDT", "UNI-BTC", "AAVE-ETH", "COMP-SOL",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ValidatePairs(pairs, 10)
	}
}

// Benchmark_getSupportedQuotes benchmarks the getSupportedQuotes function
func Benchmark_getSupportedQuotes(b *testing.B) {
	quoteSet := map[string]bool{
		"USDT": true,
		"BTC":  true,
		"ETH":  true,
		"SOL":  true,
		"BNB":  true,
		"ADA":  true,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		getSupportedQuotes(quoteSet)
	}
}
