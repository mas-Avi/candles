# Makefile

BIN_DIR=bin
PROTO_DIR=proto
OUT_DIR=gen

BINARIES=server client

.PHONY: all build clean run proto test lint docker docker-server docker-client

# Default target
all: build

# Build all binaries
build: $(BINARIES)

# Build each binary from cmd/<binary>/
$(BINARIES):
	go build -o $(BIN_DIR)/$@ ./cmd/$@

# Run: make run SERVER=server or SERVER=client
run:
	./$(BIN_DIR)/$(SERVER)

# Generate Go code from .proto files
proto:
	protoc --go_out=$(OUT_DIR) --go-grpc_out=$(OUT_DIR) --proto_path=$(PROTO_DIR) $(PROTO_DIR)/*.proto

# Clean
clean:
	go clean
	rm -rf $(BIN_DIR)/

# Tests
test:
	go test ./...

# Test coverage
test-coverage:
	go test -v -cover ./...

# Lint (requires golangci-lint)
lint:
	golangci-lint run

# Docker builds
docker: docker-server docker-client

docker-server:
	docker build -f Dockerfile.server -t candles-server .

docker-client:
	docker build -f Dockerfile.client -t candles-client .
