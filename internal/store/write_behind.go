package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrQueueFull is returned when the write-behind buffer exceeds maximum capacity.
	ErrQueueFull = errors.New("write-behind queue capacity exceeded: applying backpressure")
	// ErrQueueClosed is returned when attempting to enqueue to a terminated queue.
	ErrQueueClosed = errors.New("write-behind queue is closed")
)

// MutationType classifies the write-behind persistence operation.
type MutationType string

const (
	// OpSet represents a key-value write or update mutation.
	OpSet MutationType = "SET"
	// OpDelete represents a key removal mutation.
	OpDelete MutationType = "DELETE"
)

// Mutation represents a buffered write operation waiting for persistence.
type Mutation struct {
	Type      MutationType  `json:"type"`
	Key       string        `json:"key"`
	Value     string        `json:"value"`
	TTL       time.Duration `json:"ttl"`
	Timestamp time.Time     `json:"timestamp"`
}

// PersistentStorage abstracts the persistent backing database or disk writer.
type PersistentStorage interface {
	WriteBatch(ctx context.Context, mutations []Mutation) error
}

// BatchWriterFunc allows plain functions to act as PersistentStorage.
type BatchWriterFunc func(ctx context.Context, mutations []Mutation) error

// WriteBatch implements PersistentStorage.
func (f BatchWriterFunc) WriteBatch(ctx context.Context, mutations []Mutation) error {
	return f(ctx, mutations)
}

// WriteBehindConfig holds configuration tuning parameters for the queue.
type WriteBehindConfig struct {
	// DrainInterval determines how frequently buffered writes are flushed.
	DrainInterval time.Duration
	// BatchSize is the maximum number of coalesced mutations written per batch.
	BatchSize int
	// QueueCapacity is the maximum capacity of the in-memory mutation buffer.
	QueueCapacity int
	// CoalesceEnabled specifies whether consecutive updates to the same key are coalesced.
	CoalesceEnabled bool
	// FlushTimeout is the maximum duration allotted for each batch write call.
	FlushTimeout time.Duration
}

// DefaultWriteBehindConfig returns recommended production defaults.
func DefaultWriteBehindConfig() WriteBehindConfig {
	return WriteBehindConfig{
		DrainInterval:   50 * time.Millisecond,
		BatchSize:       500,
		QueueCapacity:   10000,
		CoalesceEnabled: true,
		FlushTimeout:    5 * time.Second,
	}
}

// WriteBehindMetrics tracks runtime telemetry for the write-behind pipeline.
type WriteBehindMetrics struct {
	TotalEnqueued  int64
	TotalCoalesced int64
	TotalPersisted int64
	TotalBatches   int64
	TotalErrors    int64
	QueueDepth     int64
}

// WriteBehindQueue provides high-throughput in-memory mutation buffering,
// write coalescing, and asynchronous batched synchronization to persistent storage.
type WriteBehindQueue struct {
	config  WriteBehindConfig
	storage PersistentStorage

	queueCh chan Mutation
	doneCh  chan struct{}
	wg      sync.WaitGroup

	closed atomic.Bool

	// Metrics counters
	totalEnqueued  atomic.Int64
	totalCoalesced atomic.Int64
	totalPersisted atomic.Int64
	totalBatches   atomic.Int64
	totalErrors    atomic.Int64

	// Mutex for flush and stop synchronization
	flushMu sync.Mutex
}

// NewWriteBehindQueue initializes an asynchronous persistence queue.
func NewWriteBehindQueue(storage PersistentStorage, config WriteBehindConfig) (*WriteBehindQueue, error) {
	if storage == nil {
		return nil, errors.New("storage must not be nil")
	}
	if config.DrainInterval <= 0 {
		config.DrainInterval = 50 * time.Millisecond
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.QueueCapacity <= 0 {
		config.QueueCapacity = 1000
	}
	if config.FlushTimeout <= 0 {
		config.FlushTimeout = 5 * time.Second
	}

	wb := &WriteBehindQueue{
		config:  config,
		storage: storage,
		queueCh: make(chan Mutation, config.QueueCapacity),
		doneCh:  make(chan struct{}),
	}

	wb.wg.Add(1)
	go wb.workerLoop()

	return wb, nil
}

// Enqueue adds a mutation to the persistence queue.
// Acknowledges immediately with sub-millisecond latency.
// Returns ErrQueueFull if maximum capacity backpressure is exceeded.
func (wb *WriteBehindQueue) Enqueue(m Mutation) error {
	if wb.closed.Load() {
		return ErrQueueClosed
	}

	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}

	select {
	case wb.queueCh <- m:
		wb.totalEnqueued.Add(1)
		return nil
	default:
		return ErrQueueFull
	}
}

// EnqueueWithContext adds a mutation, waiting if buffer is full until ctx is cancelled.
func (wb *WriteBehindQueue) EnqueueWithContext(ctx context.Context, m Mutation) error {
	if wb.closed.Load() {
		return ErrQueueClosed
	}

	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now()
	}

	select {
	case wb.queueCh <- m:
		wb.totalEnqueued.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// EnqueueSet is a helper to buffer a key-value write operation.
func (wb *WriteBehindQueue) EnqueueSet(key, value string, ttl time.Duration) error {
	return wb.Enqueue(Mutation{
		Type:      OpSet,
		Key:       key,
		Value:     value,
		TTL:       ttl,
		Timestamp: time.Now(),
	})
}

// EnqueueDelete is a helper to buffer a key deletion operation.
func (wb *WriteBehindQueue) EnqueueDelete(key string) error {
	return wb.Enqueue(Mutation{
		Type:      OpDelete,
		Key:       key,
		Timestamp: time.Now(),
	})
}

// workerLoop drains and flushes pending mutations on intervals or when buffer fills.
func (wb *WriteBehindQueue) workerLoop() {
	defer wb.wg.Done()

	ticker := time.NewTicker(wb.config.DrainInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wb.drainAndFlush()
		case <-wb.doneCh:
			// Final drain of all remaining buffered items
			wb.drainAndFlushAll()
			return
		}
	}
}

// drainAndFlush consumes up to BatchSize mutations, coalesces them, and writes to storage.
func (wb *WriteBehindQueue) drainAndFlush() {
	wb.flushMu.Lock()
	defer wb.flushMu.Unlock()

	raw := make([]Mutation, 0, wb.config.BatchSize)

	for len(raw) < wb.config.BatchSize {
		select {
		case m := <-wb.queueCh:
			raw = append(raw, m)
		default:
			goto flush
		}
	}

flush:
	if len(raw) == 0 {
		return
	}

	wb.writeMutations(raw)
}

// drainAndFlushAll ensures every remaining mutation in queueCh is written out.
func (wb *WriteBehindQueue) drainAndFlushAll() {
	wb.flushMu.Lock()
	defer wb.flushMu.Unlock()

	raw := make([]Mutation, 0, len(wb.queueCh))
	for {
		select {
		case m := <-wb.queueCh:
			raw = append(raw, m)
		default:
			if len(raw) > 0 {
				wb.writeMutations(raw)
			}
			return
		}
	}
}

// writeMutations executes coalescing and passes the batch to the backing storage.
func (wb *WriteBehindQueue) writeMutations(raw []Mutation) {
	var toWrite []Mutation

	if wb.config.CoalesceEnabled {
		// Coalesce updates preserving latest mutation per key
		coalescedMap := make(map[string]Mutation, len(raw))
		order := make([]string, 0, len(raw))

		for _, m := range raw {
			if _, exists := coalescedMap[m.Key]; !exists {
				order = append(order, m.Key)
			}
			coalescedMap[m.Key] = m
		}

		toWrite = make([]Mutation, 0, len(order))
		for _, k := range order {
			toWrite = append(toWrite, coalescedMap[k])
		}

		coalescedDiff := int64(len(raw) - len(toWrite))
		if coalescedDiff > 0 {
			wb.totalCoalesced.Add(coalescedDiff)
		}
	} else {
		toWrite = raw
	}

	if len(toWrite) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), wb.config.FlushTimeout)
	defer cancel()

	err := wb.storage.WriteBatch(ctx, toWrite)
	if err != nil {
		wb.totalErrors.Add(1)
	} else {
		wb.totalPersisted.Add(int64(len(toWrite)))
		wb.totalBatches.Add(1)
	}
}

// Flush manually triggers an immediate drain and sync of buffered mutations.
func (wb *WriteBehindQueue) Flush() {
	wb.drainAndFlush()
}

// Close gracefully stops the worker and flushes all remaining items to storage.
func (wb *WriteBehindQueue) Close() error {
	if !wb.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(wb.doneCh)
	wb.wg.Wait()
	return nil
}

// Metrics returns a snapshot of current telemetry data.
func (wb *WriteBehindQueue) Metrics() WriteBehindMetrics {
	return WriteBehindMetrics{
		TotalEnqueued:  wb.totalEnqueued.Load(),
		TotalCoalesced: wb.totalCoalesced.Load(),
		TotalPersisted: wb.totalPersisted.Load(),
		TotalBatches:   wb.totalBatches.Load(),
		TotalErrors:    wb.totalErrors.Load(),
		QueueDepth:     int64(len(wb.queueCh)),
	}
}

// String returns human-readable summary of queue status.
func (wb *WriteBehindQueue) String() string {
	m := wb.Metrics()
	return fmt.Sprintf("WriteBehindQueue(depth=%d, enqueued=%d, coalesced=%d, persisted=%d, batches=%d, errors=%d)",
		m.QueueDepth, m.TotalEnqueued, m.TotalCoalesced, m.TotalPersisted, m.TotalBatches, m.TotalErrors)
}
