// Package service provides core business logic components for the candle streaming service.
//
// The dispatcher component implements a fan-out message distribution system that efficiently
// delivers real-time candle data to multiple subscribers while handling slow clients gracefully.
package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"candles/internal/model"
	"candles/internal/utils"

	"github.com/rs/zerolog/log"
)

// Subscriber represents a client subscription to specific trading pairs.
//
// Each subscriber maintains its own buffered channel for receiving candle updates
// and a set of symbols they're interested in for efficient filtering.
type Subscriber struct {
	id                int64                 // represents a unique identifier for the subscriber
	ch                chan model.OHLCCandle // Buffered channel for candle delivery
	symbolsSubscribed map[string]struct{}   // Set of subscribed trading pairs
}

// DispatcherConfig holds configuration parameters for the Dispatcher.
type DispatcherConfig struct {
	MaxSymbolsAllowed int // Maximum symbols per subscription to prevent resource abuse
}

// Dispatcher implements a fan-out message distribution system for candle data.
//
// The dispatcher uses the actor model pattern where a single goroutine owns and manages
// all shared state (subscribers map), eliminating the need for mutexes while ensuring
// thread safety. External interactions happen through channels, making the system
// naturally concurrent and deadlock-free.
type Dispatcher struct {
	cfg              DispatcherConfig      // Configuration parameters
	subscribers      map[int64]*Subscriber // Active subscribers (owned by dispatch goroutine)
	subscriptionCh   chan *Subscriber      // Channel for new subscription requests
	unsubscriptionCh chan *Subscriber      // Channel for unsubscription requests
	started          atomic.Bool           // Atomic flag tracking dispatcher state
	randIdGen        *rand.Rand            // Random number generator for generating unique subscriber IDs
}

// NewDispatcher creates a new Dispatcher instance with the provided configuration.
func NewDispatcher(cfg DispatcherConfig) *Dispatcher {
	return &Dispatcher{
		cfg:              cfg,
		subscribers:      make(map[int64]*Subscriber),
		subscriptionCh:   make(chan *Subscriber, 10), // Buffered to prevent blocking
		unsubscriptionCh: make(chan *Subscriber, 10), // Buffered to prevent blocking
		randIdGen:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Subscribe creates a new subscription for the specified trading pairs.
//
// This method validates the requested trading pairs and creates a new Subscriber
// if validation passes. The subscription request is sent to the dispatcher goroutine
// via a channel to ensure thread-safe addition to the subscribers map.
func (b *Dispatcher) Subscribe(pairs []string) (*Subscriber, error) {
	if !b.started.Load() {
		return nil, errors.New("dispatcher not started")
	}

	if err := utils.ValidatePairs(pairs, b.cfg.MaxSymbolsAllowed); err != nil {
		return nil, err
	}

	symSet := make(map[string]struct{}, len(pairs))
	for _, s := range pairs {
		symSet[s] = struct{}{}
	}

	sub := &Subscriber{
		id:                b.randIdGen.Int63(),              // Generate a unique ID for the subscriber
		ch:                make(chan model.OHLCCandle, 100), // buffer size per client
		symbolsSubscribed: symSet,
	}

	// write to channel, return error if blocked
	select {
	case b.subscriptionCh <- sub:
	default:
		b.unsubscriptionCh <- sub // If channel is full, unsubscribe the user
		return nil, fmt.Errorf("subscription channel is full, will unsubscribe user")
	}

	return sub, nil
}

// subscribe is an internal method that adds a subscriber to the active subscribers map.
func (b *Dispatcher) subscribe(subscriber *Subscriber) {
	b.subscribers[subscriber.id] = subscriber
}

// Unsubscribe removes a subscriber from the dispatcher.
func (b *Dispatcher) Unsubscribe(sub *Subscriber) error {
	// write to channel, return error if blocked
	select {
	case b.unsubscriptionCh <- sub:
		return nil
	default:
		return fmt.Errorf("subscription channel is full")
	}
}

// unsubscribe is an internal method that removes a subscriber and cleans up resources.
func (b *Dispatcher) unsubscribe(sub *Subscriber) {
	if _, ok := b.subscribers[sub.id]; ok {
		delete(b.subscribers, sub.id)
		close(sub.ch)
	}
}

// StartDispatching starts the main dispatcher goroutine that handles all subscriber management
// and message distribution.
//
// This method implements the actor model pattern where a single goroutine owns and manages
// all shared state. The goroutine processes requests from three sources:
//  1. Context cancellation for graceful shutdown
//  2. Subscription/unsubscription requests via channels
//  3. Incoming candle data for distribution
func (b *Dispatcher) StartDispatching(ctx context.Context, candleCh <-chan model.OHLCCandle) error {
	if !b.started.CompareAndSwap(false, true) {
		return errors.New("dispatcher already started")
	}

	go func() {

		defer func() {
			// Cleanup on shutdown
			for _, sub := range b.subscribers {
				close(sub.ch)
			}
			b.subscribers = make(map[int64]*Subscriber)
		}()

		for {
			select {
			case <-ctx.Done():
				log.Info().Msgf("dispatcher stopped")
				return
			case sub := <-b.subscriptionCh:
				b.subscribe(sub)
			case sub := <-b.unsubscriptionCh:
				b.unsubscribe(sub)
			case candle := <-candleCh:
				b.dispatch(candle)
			}
		}
	}()
	return nil
}

// dispatch distributes a candle to all interested subscribers.
//
// This method is only called from within the dispatcher goroutine, ensuring thread-safe
// access to the subscribers map without requiring mutex protection.
//
// Behavior for slow clients:
//   - If subscriber channel is full, drops oldest buffered candle
//   - Ensures new candle is always delivered (replacing oldest)
func (b *Dispatcher) dispatch(candle model.OHLCCandle) {
	for _, sub := range b.subscribers {
		if _, ok := sub.symbolsSubscribed[candle.Pair]; ok {
			select {
			case sub.ch <- candle:
				// Successfully delivered without blocking
			default:
				// channel full → drop tick for slow client
				log.Info().Any("subscriber", sub).Msg("subscriber is too slow, dropping oldest buffered candle")
				<-sub.ch         // Remove oldest candle
				sub.ch <- candle // Add new candle
			}
		}
	}
}
