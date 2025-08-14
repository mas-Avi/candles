package exchange

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateConfig(t *testing.T) {
	defaultCfg := &ExchangeConfig{
		BaseURL:    "wss://default.com",
		MaxSymbols: 10,
	}

	tests := []struct {
		name      string
		config    *ExchangeConfig
		wantError bool
	}{
		{
			name: "valid config",
			config: &ExchangeConfig{
				BaseURL:    "wss://test.com",
				MaxSymbols: 5,
			},
			wantError: false,
		},
		{
			name: "empty BaseURL uses default",
			config: &ExchangeConfig{
				MaxSymbols: 5,
			},
			wantError: false,
		},
		{
			name: "zero MaxSymbols uses default",
			config: &ExchangeConfig{
				BaseURL: "wss://test.com",
			},
			wantError: false,
		},
		{
			name: "negative MaxSymbols uses default",
			config: &ExchangeConfig{
				BaseURL:    "wss://test.com",
				MaxSymbols: -1,
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateConfig(tt.config, defaultCfg)

			if tt.wantError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			if tt.config.BaseURL == "" {
				assert.Equal(t, defaultCfg.BaseURL, tt.config.BaseURL)
			}
			if tt.config.MaxSymbols <= 0 {
				assert.Equal(t, defaultCfg.MaxSymbols, tt.config.MaxSymbols)
			}
		})
	}
}

func TestErrorConstants(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "ErrInvalidConfig message",
			err:      ErrInvalidConfig,
			expected: "invalid configuration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.err.Error())
		})
	}
}

func TestErrorWrapping(t *testing.T) {
	t.Run("wrapped ErrInvalidConfig", func(t *testing.T) {
		wrappedErr := errors.New("wrapped: invalid configuration")
		assert.False(t, errors.Is(wrappedErr, ErrInvalidConfig))

		// Test proper wrapping
		properWrapped := fmt.Errorf("%w: additional context", ErrInvalidConfig)
		assert.True(t, errors.Is(properWrapped, ErrInvalidConfig))
	})
}

func TestValidateConfigMutability(t *testing.T) {
	defaultCfg := &ExchangeConfig{
		BaseURL:    "wss://default.com",
		MaxSymbols: 100,
	}

	t.Run("config modified in place", func(t *testing.T) {
		cfg := &ExchangeConfig{
			// BaseURL and MaxSymbols intentionally empty/zero
		}

		originalBaseURL := cfg.BaseURL
		originalMaxSymbols := cfg.MaxSymbols

		err := validateConfig(cfg, defaultCfg)
		assert.NoError(t, err)

		// Verify the config was modified in place
		assert.NotEqual(t, originalBaseURL, cfg.BaseURL)
		assert.NotEqual(t, originalMaxSymbols, cfg.MaxSymbols)
		assert.Equal(t, defaultCfg.BaseURL, cfg.BaseURL)
		assert.Equal(t, defaultCfg.MaxSymbols, cfg.MaxSymbols)
	})

	t.Run("config not modified when values present", func(t *testing.T) {
		cfg := &ExchangeConfig{
			BaseURL:    "wss://custom.com",
			MaxSymbols: 50,
		}

		originalBaseURL := cfg.BaseURL
		originalMaxSymbols := cfg.MaxSymbols

		err := validateConfig(cfg, defaultCfg)
		assert.NoError(t, err)

		// Verify the config was NOT modified
		assert.Equal(t, originalBaseURL, cfg.BaseURL)
		assert.Equal(t, originalMaxSymbols, cfg.MaxSymbols)
	})
}

func TestValidateConfigWithNilDefault(t *testing.T) {

	t.Run("nil default config with empty values", func(t *testing.T) {
		cfg := &ExchangeConfig{}

		// This would panic if not handled properly
		assert.Panics(t, func() {
			validateConfig(cfg, nil)
		})
	})

	t.Run("nil default config with preset values", func(t *testing.T) {
		cfg := &ExchangeConfig{
			BaseURL:    "wss://test.com",
			MaxSymbols: 10,
		}

		// Should not panic since we don't access defaults
		assert.NotPanics(t, func() {
			err := validateConfig(cfg, nil)
			assert.NoError(t, err)
		})
	})
}

func TestExchangeConfigStructure(t *testing.T) {

	t.Run("zero value config", func(t *testing.T) {
		var cfg ExchangeConfig
		assert.Empty(t, cfg.BaseURL)
		assert.Zero(t, cfg.MaxSymbols)
	})

	t.Run("config field assignment", func(t *testing.T) {
		cfg := &ExchangeConfig{}

		cfg.BaseURL = "wss://test.com"
		cfg.MaxSymbols = 25

		assert.Equal(t, "wss://test.com", cfg.BaseURL)
		assert.Equal(t, 25, cfg.MaxSymbols)
	})
}

func TestValidateConfigBoundaryValues(t *testing.T) {
	defaultCfg := &ExchangeConfig{
		BaseURL:    "wss://default.com",
		MaxSymbols: 10,
	}

	tests := []struct {
		name           string
		maxSymbols     int
		expectedResult int
		description    string
	}{
		{
			name:           "zero MaxSymbols",
			maxSymbols:     0,
			expectedResult: 10,
			description:    "should use default",
		},
		{
			name:           "negative MaxSymbols",
			maxSymbols:     -1,
			expectedResult: 10,
			description:    "should use default",
		},
		{
			name:           "large negative MaxSymbols",
			maxSymbols:     -999,
			expectedResult: 10,
			description:    "should use default",
		},
		{
			name:           "minimum positive MaxSymbols",
			maxSymbols:     1,
			expectedResult: 1,
			description:    "should keep original value",
		},
		{
			name:           "large positive MaxSymbols",
			maxSymbols:     9999,
			expectedResult: 9999,
			description:    "should keep original value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &ExchangeConfig{
				BaseURL:    "wss://test.com",
				MaxSymbols: tt.maxSymbols,
			}

			err := validateConfig(cfg, defaultCfg)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedResult, cfg.MaxSymbols, tt.description)
		})
	}
}

func TestValidateConfigBaseURLHandling(t *testing.T) {
	defaultCfg := &ExchangeConfig{
		BaseURL:    "wss://default-exchange.com",
		MaxSymbols: 10,
	}

	tests := []struct {
		name           string
		baseURL        string
		expectedResult string
		description    string
	}{
		{
			name:           "empty BaseURL",
			baseURL:        "",
			expectedResult: "wss://default-exchange.com",
			description:    "should use default",
		},
		{
			name:           "whitespace BaseURL",
			baseURL:        "   ",
			expectedResult: "   ",
			description:    "should keep whitespace (validation doesn't trim)",
		},
		{
			name:           "valid BaseURL",
			baseURL:        "wss://custom-exchange.com",
			expectedResult: "wss://custom-exchange.com",
			description:    "should keep original value",
		},
		{
			name:           "invalid but non-empty BaseURL",
			baseURL:        "not-a-url",
			expectedResult: "not-a-url",
			description:    "should keep original value (URL validation not implemented)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &ExchangeConfig{
				BaseURL:    tt.baseURL,
				MaxSymbols: 5,
			}

			err := validateConfig(cfg, defaultCfg)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedResult, cfg.BaseURL, tt.description)
		})
	}
}

func TestValidateConfigOrderOfOperations(t *testing.T) {
	defaultCfg := &ExchangeConfig{
		BaseURL:    "wss://default.com",
		MaxSymbols: 20,
	}

	t.Run("defaults applied after logger validation passes", func(t *testing.T) {
		cfg := &ExchangeConfig{
			// BaseURL and MaxSymbols will get defaults
		}

		err := validateConfig(cfg, defaultCfg)
		assert.NoError(t, err)

		// Verify defaults were applied
		assert.Equal(t, defaultCfg.BaseURL, cfg.BaseURL)
		assert.Equal(t, defaultCfg.MaxSymbols, cfg.MaxSymbols)
	})
}
