package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Shard defines the interface for a cache shard or store that supports TTL sweeping.
type Shard interface {
	SweepExpired(sampleSize int) int
}

// Config holds configuration parameters for the TTL sweeper.
type Config struct {
	Interval   time.Duration
	SampleSize int
}

// Option applies configuration options to TTLSweeper.
type Option func(*TTLSweeper)

// WithInterval configures the sweeping interval.
func WithInterval(interval time.Duration) Option {
	return func(s *TTLSweeper) {
		if interval > 0 {
			s.interval = interval
		}
	}
}

// WithSampleSize configures the maximum number of keys sampled per shard per tick.
func WithSampleSize(sampleSize int) Option {
	return func(s *TTLSweeper) {
		if sampleSize > 0 {
			s.sampleSize = sampleSize
		}
	}
}

// TTLSweeper manages active key TTL expiration across one or more cache shards.
type TTLSweeper struct {
	shards     []Shard
	interval   time.Duration
	sampleSize int

	totalEvicted uint64
	running      uint32
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewTTLSweeper creates a new TTLSweeper worker instance.
func NewTTLSweeper(shards []Shard, opts ...Option) *TTLSweeper {
	s := &TTLSweeper{
		shards:     shards,
		interval:   100 * time.Millisecond,
		sampleSize: 20,
		stopCh:     make(chan struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start launches the active TTL sweeper background worker loop.
func (s *TTLSweeper) Start(ctx context.Context) {
	if !atomic.CompareAndSwapUint32(&s.running, 0, 1) {
		return
	}

	s.wg.Add(1)
	go s.run(ctx)
}

// Stop gracefully signals the background worker loop to stop and waits for completion.
func (s *TTLSweeper) Stop() {
	if !atomic.CompareAndSwapUint32(&s.running, 1, 0) {
		return
	}
	close(s.stopCh)
	s.wg.Wait()
}

// TotalEvicted returns the cumulative total number of keys evicted by active sweeps.
func (s *TTLSweeper) TotalEvicted() uint64 {
	return atomic.LoadUint64(&s.totalEvicted)
}

// IsRunning reports whether the background worker is currently active.
func (s *TTLSweeper) IsRunning() bool {
	return atomic.LoadUint32(&s.running) == 1
}

// SweepOnce manually executes a single sweep round over all configured shards.
func (s *TTLSweeper) SweepOnce() uint64 {
	var evictedThisRound uint64
	for _, shard := range s.shards {
		if shard == nil {
			continue
		}
		evicted := shard.SweepExpired(s.sampleSize)
		evictedThisRound += uint64(evicted)
	}
	if evictedThisRound > 0 {
		atomic.AddUint64(&s.totalEvicted, evictedThisRound)
	}
	return evictedThisRound
}

func (s *TTLSweeper) run(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			atomic.StoreUint32(&s.running, 0)
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.SweepOnce()
		}
	}
}
