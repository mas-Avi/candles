# Candles - Real-Time Cryptocurrency Candle Streaming Service

A high-performance, real-time cryptocurrency candle (OHLCV) streaming service built in Go, designed to aggregate trading data and distribute it to multiple subscribers via gRPC streaming.

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Building](#building)
- [Running](#running)
- [Docker Support](#docker-support)
- [Design Choices](#design-choices)
- [Missing Features](#missing-features)

## Overview

The Candles service is a system that:

1. **Connects to cryptocurrency exchanges** via WebSocket
2. **Aggregates raw trade data** into time-based OHLCV candles
3. **Distributes candle data** to multiple clients via gRPC streaming

## Project Structure

### Key Directories Explained

#### `/cmd/`
Application entry points following Go project layout standards. Each subdirectory represents a different executable.

#### `/internal/`
Private application code that cannot be imported by other applications. This enforces proper encapsulation and API boundaries.

- **`aggregator/`** - Converts raw trade data into OHLCV candles using time-based windows
- **`exchange/`** - Handles exchange-specific connectivity, currently supporting Binance with extensible architecture
- **`model/`** - Core data structures used throughout the application
- **`service/`** - Business logic and gRPC service implementation
- **`utils/`** - Shared utilities for validation and common operations
- **`websocket/`** - Generic WebSocket client with error handling

#### `/proto/`
Protocol Buffer definitions and generated code for gRPC communication.

#### `/test/`
Test files organized by type:
- **`integration/`** - End-to-end tests with real component interactions
- **`testdata/`** - Static test data files
- **`mocks/`** - Mock implementations for testing


## Architecture

### Data Flow

	Exchange 1 ─┐
	Exchange 2 ─┼─→ Fan-In ─→ Trade Processing ─→ Candle Aggregation ─→ Output
	Exchange N ─┘               (goroutines)         (time-based)      (channel)

### 🗂️ Project Structure 

```
.
├── cmd/
│   ├── server/
│   │   └── main.go
│   └── client/
│       └── main.go
├── proto/
│   └── *.proto
├── gen/                        # generated protobuf Go files
├── bin/                        # compiled binaries
├── internal/                   # main project files
│   ├── candles/
│   │   └── aggregator.go       # aggregates flows from multiple exchanges into a single stream
│   └── exchange/               # holds exchange specific integrations
│       └── binance.go      
│       └── coinbase.go         
│       └── okx.go              
│   └── model/
│       └── types.go            # general data types used by the project
│   └── service/
│       └── candles_service.go  # grpc service to serve candles to subscribers
│       └── dispatcher.go       # manages subscribers and dispatches candles to them
│   └── utils/
│       └── utils.go            
│   └── websocket/
│       └── websocket_client.go # manages websocket connections for the exchange integrations
├── Makefile
├── Dockerfile.server
├── Dockerfile.client
└── go.mod
```

## Prerequisites

- Go 1.18+
- `make`
- `protoc` (for proto generation)
- Docker (for image builds)
- Unix-like shell (Linux/macOS/Git Bash/WSL)


## Building

This project includes a `Makefile` to automate common development tasks such as building binaries, generating protobufs, running tests, linting, and building Docker images.

### 🔨 Build all binaries
```bash
make
# or
make build
```
Builds:
- `cmd/server` → `bin/server`
- `cmd/client` → `bin/client`


### 🧬 Generate protobuf files
```bash
make proto
```
Requires:
- `protoc`
- `protoc-gen-go`
- `protoc-gen-go-grpc`

Install plugins:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### 🧹 Clean build artifacts
```bash
make clean
```

## Running

### ▶️ Run a binary
```bash
make run SERVER=server
make run SERVER=client
```
> Note: binaries must be built first.

### 🧪 Run tests
```bash
make test
```

### ✅ Lint code
```bash
make lint
```
Requires `golangci-lint`:
```bash
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

---

## Docker Support

### 🐳  Build Docker images
```bash
make docker           # builds both server and client images
make docker-server    # builds only server image
make docker-client    # builds only client image
```

Requires:
- Docker installed and running
- `Dockerfile.server` and `Dockerfile.client` in project root

---

## Design Choices

### TradeEvent / OHLC / Candle

Have used decimal.Decimal to represent the OHLCV. This is because I have preferred compuational accuracy over speed.

For performance improvements, it would be better to use fixed-point arithmetic.

Used string for serializing and sending Candles to subscribers, this is to ensure there is no accidental precision issues in the send.

### Exchange connectors

- An exchange connector is able to create websockets with x no of pairs subscribed. This allows the caller to decide how it wants to split/combine trading pairs and websocket connections. Whether 1 pairs per conneciton or all pairs per conneciton.

### Aggregator

- Uses the idiomatic fan-in pattern to merge the exchange connections into a single stream. Alternative is using a ring buffer which is preferred for lower latency/performance.

- Have not sharded any of the compuational path by the subscribed pair. This can be done with minor changes to the program. However, if we do the other things right, this is usually not needed.

### Latency

This project has not been optimized for low-latency (sub-milliseconds). 
I preferred a more idiomatic Go implemenation although I do use the actor model at times to remove the need for mutexes (dispatcher.go).

If we did need to support sub-milliseconds, there would be various things that would need to be done:
- modify the fan-in function to use a ring buffer like the Disruptor. This would allow us to merge the various exchange stream more efficiently.
- use sync.Pool to reuse objects such as the TradeEvent, OHLCCandle and Candle. This reduces GC pressure and those pauses.
- remove any blocking sends, ensure the buffers are adequate and have a proper strategy for slow consumers.
- ensure the hotpath only contains immediately relevant computations and anything secondary is done on another thread (e.g. logging)
- modify the way json is currently validated


The main way to improve latency is first to measure, which is where a good metrics implementation is required.

---

## Missing Features

### Websocket

#### Websocket reconnection
Did not get the time to do this, unfortunately.

#### Websocket subscription
I assume the subscriptions are successful. There is no proper handling of subscription issues.

### Metrics / Observability

Felt this was out of scope and would add complexity that was not required for this project.

### Stale message handling

Realised this a bit late, and did not have time to implement.

### Robust configuration and logging

Again, felt this was out of scope.

### Integration tests

Unfortunately, did not get time to implement integration tests. The project is designed to be testable, but the actual tests are missing.