/*
Package main implements a gRPC server for streaming real-time candlestick data.

This server aggregates candlestick data from multiple cryptocurrency exchanges
(Binance, Coinbase, OKX) and provides a streaming gRPC API for clients to
subscribe to real-time candle data. It supports graceful shutdown, health checks,
and configurable candle intervals.

The server connects to multiple exchanges via WebSocket, aggregates the data
into consistent candle intervals, and broadcasts updates to subscribed clients.

Usage:

	go run main.go -port=:50051 -interval=5 -symbols=BTC-USDT,ETH-USDT

The server will start listening for gRPC connections and begin streaming
candlestick data for the specified symbols at the configured interval.
*/
package main

import (
	pb "candles/gen/proto"
	"candles/internal/candles"
	"candles/internal/exchange"
	"candles/internal/service"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

// Command-line flags for configuring the server behavior
var (
	// port specifies the TCP port for the gRPC server to listen on
	port = flag.String("port", ":50051", "The server port")
	// interval defines the candlestick aggregation interval in seconds
	interval = flag.Int("interval", 5, "Candle interval in seconds")
	// symbols contains the comma-separated list of trading pairs to track
	symbols = flag.String("symbols", "BTC-USDT,ETH-USDT,SOL-USDT", "Comma-separated list of symbols")
)

// main is the entry point of the candle server application.
// It initializes exchange connectors, starts data aggregation, sets up the gRPC server,
// and handles graceful shutdown. The server will continuously stream candlestick data
// to connected clients until shutdown.
func main() {
	// Parse command-line flags
	flag.Parse()

	// Initialize structured logger with timestamp and info level
	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// Validate configuration parameters before proceeding
	if err := validateConfig(); err != nil {
		log.Fatal().Err(err).Msg("invalid configuration")
	}

	// Create context for managing application lifecycle and graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize the candle service with exchange connectors and aggregation logic
	candleService, err := newCandleService()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initiate candle service")
	}

	// Start data collection from exchanges for the specified trading symbols
	symbolList := strings.Split(*symbols, ",")
	if err := candleService.Start(ctx, symbolList); err != nil {
		log.Fatal().Err(err).Msg("failed to start candle service")
	}
	defer candleService.Stop()

	// Set up TCP listener for gRPC server
	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to listen")
	}

	// Create gRPC server with keepalive parameters for connection management
	// These settings help maintain stable long-lived streaming connections
	s := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,  // Close idle connections after 5 minutes
			MaxConnectionAge:  30 * time.Minute, // Force reconnection after 30 minutes
			Time:              20 * time.Second, // Send keepalive pings every 20 seconds
			Timeout:           10 * time.Second, // Wait 10 seconds for ping response
		}),
	)

	// Register the candle streaming service
	pb.RegisterCandleServiceServer(s, candleService)

	// Register health check service for monitoring and load balancing
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// Set up signal handling for graceful shutdown
	// This ensures proper cleanup of connections and resources when the server
	// receives interrupt signals like Ctrl+C or SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Info().Msg("initiating graceful shutdown")
		cancel()         // Cancel context to stop data collection
		s.GracefulStop() // Stop accepting new requests and finish existing ones
		lis.Close()      // Close the network listener
	}()

	// Log server startup information
	log.Info().
		Str("port", *port).
		Int("interval", *interval).
		Strs("symbols", symbolList).
		Msg("server starting")

	// Start serving gRPC requests - this blocks until shutdown
	if err := s.Serve(lis); err != nil {
		log.Fatal().Err(err).Msg("failed to serve")
	}
}

// validateConfig performs validation of command-line configuration parameters.
// It ensures that all required settings are properly configured before the server
// attempts to start.
//
// Returns an error if any validation fails, nil otherwise.
func validateConfig() error {
	// Ensure port is specified
	if port == nil || *port == "" {
		return fmt.Errorf("port cannot be empty")
	}
	// Ensure interval is positive
	if *interval <= 0 {
		return fmt.Errorf("interval must be greater than 0")
	}
	// Ensure symbols list is not empty
	if symbols == nil || *symbols == "" {
		return fmt.Errorf("symbols list cannot be empty")
	}
	return nil
}

// newCandleService initializes and configures the candle service with all necessary
// exchange connectors and aggregation logic.
//
// It creates connectors for Binance, Coinbase, and OKX exchanges, sets up the
// data aggregator with the specified interval, and configures the broadcaster
// for distributing data to clients.
//
// Parameters:
//   - log: Logger instance for structured logging
//
// Returns:
//   - *service.CandleService: Configured candle service ready to start
//   - error: Any error encountered during initialization
func newCandleService() (*service.CandleService, error) {
	// Convert interval from seconds to duration for aggregator
	intervalDuration := time.Duration(*interval) * time.Second

	// Initialize Binance exchange connector
	binanceConnector, err := exchange.NewBinanceConnector(nil)
	if err != nil {
		log.Error().Err(err).Msg("failed to create Binance connector")
		return nil, err
	}

	// Initialize Coinbase exchange connector
	coinbaseConnector, err := exchange.NewCoinbaseConnector(nil)
	if err != nil {
		log.Error().Err(err).Msg("failed to create Coinbase connector")
		return nil, err
	}

	// Initialize OKX exchange connector
	okxConnector, err := exchange.NewOkxConnector(nil)
	if err != nil {
		log.Error().Err(err).Msg("failed to create OKX connector")
		return nil, err
	}

	// Collect all exchange connectors for the aggregator
	exchanges := []candles.ExchangeConnector{binanceConnector, coinbaseConnector, okxConnector}

	// Create data aggregator that combines data from all exchanges
	aggregator := candles.NewAggregator(exchanges, intervalDuration)

	// Create broadcaster/dispatcher for managing client subscriptions
	broadcaster := service.NewDispatcher(service.DispatcherConfig{
		MaxSymbolsAllowed: 100, // Limit concurrent symbol subscriptions
	})

	// Create and return the complete candle service
	return service.NewCandleService(broadcaster, aggregator), nil
}
