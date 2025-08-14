/*
Package main implements a gRPC client for subscribing to real-time candlestick data.

This client connects to a candle service server and streams live candlestick data
for specified cryptocurrency trading pairs. It supports graceful shutdown via
OS signals and provides structured logging of received candle data.

Usage:

	go run main.go -addr=localhost:50051 -symbols=BTC-USDT,ETH-USDT

The client will continuously receive and log candlestick data until interrupted.
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	pb "candles/gen/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Command-line flags for configuring the client connection and subscription
var (
	// serverAddr specifies the gRPC server address to connect to
	serverAddr = flag.String("addr", "localhost:50051", "The server address in the format host:port")
	// symbols contains the comma-separated list of trading pairs to subscribe to
	symbols = flag.String("symbols", "BTC-USDT,ETH-USDT,SOL-USDT", "Comma-separated list of symbols to subscribe to")
)

// main is the entry point of the candle client application.
// It establishes a gRPC connection to the candle service, subscribes to
// specified symbols, and continuously receives and logs candlestick data.
func main() {
	// Parse command-line flags
	flag.Parse()

	// Initialize structured logger with timestamp and info level
	log := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()

	// Validate configuration before proceeding
	err := validateConfig()
	if err != nil {
		log.Fatal().Err(err).Msg("Configuration error")
	}

	// Create context for managing application lifecycle and cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	// This allows the client to properly close connections when receiving
	// interrupt signals like Ctrl+C or SIGTERM
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Info().Msg("received shutdown signal")
		cancel()
	}()

	// Configure gRPC connection options
	// Using insecure credentials for development/testing purposes
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithTimeout(5 * time.Second),
	}

	// Establish gRPC connection to the candle service
	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		log.Fatal().Err(err).Msg("did not connect")
	}
	defer conn.Close()

	// Create candle service client from the connection
	client := pb.NewCandleServiceClient(conn)

	// Prepare subscription request with the specified symbols
	symbolList := strings.Split(*symbols, ",")
	req := &pb.SubscriptionRequest{
		Symbols: symbolList,
	}

	log.Info().Msgf("Subscribing to symbols: %v", symbolList)

	// Start streaming candlestick data
	stream, err := client.Subscribe(ctx, req)
	if err != nil {
		log.Fatal().Err(err).Msg("could not subscribe")
	}

	// Main message receiving loop
	// Continuously receive and process candlestick data until stream ends
	// or context is cancelled
	for {
		candle, err := stream.Recv()
		if err == io.EOF {
			log.Info().Msg("stream has closed")
			break
		}
		if err != nil {
			log.Fatal().Err(err).Msg("failed to receive candle")
		}

		// Process and format timestamps for human-readable output
		nowTimestamp := time.Now().Format(time.RFC3339)

		// Parse Unix timestamps from the candle data
		startTime, err := strconv.ParseInt(candle.StartTimestamp, 10, 64)
		if err != nil {
			log.Error().Err(err).Msg("failed to parse start timestamp")
		}
		endTime, err := strconv.ParseInt(candle.EndTimestamp, 10, 64)
		if err != nil {
			log.Error().Err(err).Msg("failed to parse end timestamp")
		}

		// Convert Unix timestamps to RFC3339 format for readability
		startTimestamp := time.Unix(startTime, 0).Format(time.RFC3339)
		endTimestamp := time.Unix(endTime, 0).Format(time.RFC3339)

		// Log the complete candle data with structured fields
		log.Info().
			Str("pair", candle.Symbol).
			Str("now", nowTimestamp).
			Str("open", candle.Open).
			Str("high", candle.High).
			Str("low", candle.Low).
			Str("close", candle.Close).
			Str("start_time", startTimestamp).
			Str("end_time", endTimestamp).
			Msg("received candle")
	}
}

// validateConfig performs validation of command-line configuration.
// It ensures that required parameters are properly set before the client
// attempts to connect to the server.
//
// Returns an error if any validation fails, nil otherwise.
func validateConfig() error {
	// Ensure symbols list is not empty
	if len(*symbols) == 0 {
		return fmt.Errorf("symbols list cannot be empty")
	}
	// Ensure server address is provided
	if *serverAddr == "" {
		return fmt.Errorf("server address cannot be empty")
	}
	return nil
}
