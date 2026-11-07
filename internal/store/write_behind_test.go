package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStorage captures batches for assertion in unit tests.
type mockStorage struct {
	mu      sync.Mutex
	batches [][]Mutation
	records map[string]Mutation
	err     error
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		records: make(map[string]Mutation),
	}
}

func (m *mockStorage) WriteBatch(ctx context.Context, mutations []Mutation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return m.err
	}

	batchCopy := make([]Mutation, len(mutations))
	copy(batchCopy, mutations)
	m.batches = append(m.batches, batchCopy)

	for _, mut := range mutations {
		m.records[mut.Key] = mut
	}
	return nil
}

func (m *mockStorage) getRecords() map[string]Mutation {
	m.mu.Lock()
	defer m.mu.Unlock()

	res := make(map[string]Mutation, len(m.records))
	for k, v := range m.records {
		res[k] = v
	}
	return res
}

func (m *mockStorage) getBatchCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.batches)
}

func TestWriteBehindBasicEnqueueAndFlush(t *testing.T) {
	storage := newMockStorage()
	config := WriteBehindConfig{
		DrainInterval:   20 * time.Millisecond,
		BatchSize:       10,
		QueueCapacity:   100,
		CoalesceEnabled: true,
	}

	wb, err := NewWriteBehindQueue(storage, config)
	require.NoError(t, err)
	defer wb.Close()

	start := time.Now()
	err = wb.EnqueueSet("user:100", "Alice", 10*time.Minute)
	assert.NoError(t, err)
	assert.True(t, time.Since(start) < 5*time.Millisecond, "enqueue should return with sub-millisecond latency")

	err = wb.EnqueueDelete("user:999")
	assert.NoError(t, err)

	// Wait for automatic drain
	time.Sleep(50 * time.Millisecond)

	records := storage.getRecords()
	assert.Equal(t, 2, len(records))
	assert.Equal(t, "Alice", records["user:100"].Value)
	assert.Equal(t, OpDelete, records["user:999"].Type)

	metrics := wb.Metrics()
	assert.Equal(t, int64(2), metrics.TotalEnqueued)
	assert.Equal(t, int64(2), metrics.TotalPersisted)
	assert.Equal(t, int64(0), metrics.TotalErrors)
}

func TestWriteBehindCoalescing(t *testing.T) {
	storage := newMockStorage()
	config := WriteBehindConfig{
		DrainInterval:   100 * time.Millisecond, // Longer interval to allow queuing
		BatchSize:       50,
		QueueCapacity:   100,
		CoalesceEnabled: true,
	}

	wb, err := NewWriteBehindQueue(storage, config)
	require.NoError(t, err)
	defer wb.Close()

	// Rapidly enqueue multiple updates to the same key
	err = wb.EnqueueSet("session:abc", "v1", 0)
	require.NoError(t, err)
	err = wb.EnqueueSet("session:abc", "v2", 0)
	require.NoError(t, err)
	err = wb.EnqueueSet("session:abc", "v3-final", 0)
	require.NoError(t, err)

	// Another key
	err = wb.EnqueueSet("session:xyz", "token-xyz", 0)
	require.NoError(t, err)

	// Manually flush
	wb.Flush()

	records := storage.getRecords()
	assert.Equal(t, 2, len(records), "only 2 unique keys should be persisted after coalescing")
	assert.Equal(t, "v3-final", records["session:abc"].Value)
	assert.Equal(t, "token-xyz", records["session:xyz"].Value)

	metrics := wb.Metrics()
	assert.Equal(t, int64(4), metrics.TotalEnqueued)
	assert.Equal(t, int64(2), metrics.TotalCoalesced)
	assert.Equal(t, int64(2), metrics.TotalPersisted)
}

func TestWriteBehindDeleteCoalescing(t *testing.T) {
	storage := newMockStorage()
	config := WriteBehindConfig{
		DrainInterval:   200 * time.Millisecond,
		BatchSize:       10,
		QueueCapacity:   50,
		CoalesceEnabled: true,
	}

	wb, err := NewWriteBehindQueue(storage, config)
	require.NoError(t, err)
	defer wb.Close()

	// Set then delete
	require.NoError(t, wb.EnqueueSet("temp:1", "val1", 0))
	require.NoError(t, wb.EnqueueDelete("temp:1"))

	// Delete then set
	require.NoError(t, wb.EnqueueDelete("temp:2"))
	require.NoError(t, wb.EnqueueSet("temp:2", "resurrected", 0))

	wb.Flush()

	records := storage.getRecords()
	assert.Equal(t, OpDelete, records["temp:1"].Type)
	assert.Equal(t, OpSet, records["temp:2"].Type)
	assert.Equal(t, "resurrected", records["temp:2"].Value)
}

func TestWriteBehindCapacityBackpressure(t *testing.T) {
	storage := newMockStorage()
	// Set very small capacity and pause drain
	config := WriteBehindConfig{
		DrainInterval: 1 * time.Hour,
		BatchSize:     10,
		QueueCapacity: 3,
	}

	wb, err := NewWriteBehindQueue(storage, config)
	require.NoError(t, err)
	defer wb.Close()

	// Fill buffer to capacity
	assert.NoError(t, wb.EnqueueSet("k1", "v1", 0))
	assert.NoError(t, wb.EnqueueSet("k2", "v2", 0))
	assert.NoError(t, wb.EnqueueSet("k3", "v3", 0))

	// Next non-blocking enqueue must fail with ErrQueueFull
	err = wb.EnqueueSet("k4", "v4", 0)
	assert.ErrorIs(t, err, ErrQueueFull)

	// Context with timeout should also fail when queue remains full
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err = wb.EnqueueWithContext(ctx, Mutation{Type: OpSet, Key: "k4", Value: "v4"})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestWriteBehindGracefulShutdownZeroDataLoss(t *testing.T) {
	storage := newMockStorage()
	config := WriteBehindConfig{
		DrainInterval:   10 * time.Hour, // Will not drain automatically before Close
		BatchSize:       50,
		QueueCapacity:   500,
		CoalesceEnabled: false,
	}

	wb, err := NewWriteBehindQueue(storage, config)
	require.NoError(t, err)

	totalItems := 100
	for i := 0; i < totalItems; i++ {
		require.NoError(t, wb.EnqueueSet(fmt.Sprintf("item-%d", i), fmt.Sprintf("val-%d", i), 0))
	}

	// Close immediately - must flush all 100 items before returning
	err = wb.Close()
	require.NoError(t, err)

	records := storage.getRecords()
	assert.Equal(t, totalItems, len(records), "all items must be flushed on Close without data loss")

	// Post-close enqueue must be rejected
	err = wb.EnqueueSet("late", "item", 0)
	assert.ErrorIs(t, err, ErrQueueClosed)
}

func TestWriteBehindConcurrentProducers(t *testing.T) {
	storage := newMockStorage()
	config := WriteBehindConfig{
		DrainInterval:   10 * time.Millisecond,
		BatchSize:       100,
		QueueCapacity:   2000,
		CoalesceEnabled: true,
	}

	wb, err := NewWriteBehindQueue(storage, config)
	require.NoError(t, err)
	defer wb.Close()

	numWorkers := 10
	itemsPerWorker := 50
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				key := fmt.Sprintf("worker-%d-key-%d", workerID, i%5) // create duplicate keys for coalescing
				val := fmt.Sprintf("val-%d", i)
				_ = wb.EnqueueSet(key, val, 0)
			}
		}(w)
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	wb.Flush()

	metrics := wb.Metrics()
	assert.Equal(t, int64(numWorkers*itemsPerWorker), metrics.TotalEnqueued)
	assert.True(t, metrics.TotalPersisted > 0)
	assert.True(t, metrics.TotalCoalesced > 0)
}

func TestWriteBehindStorageErrorTracking(t *testing.T) {
	storage := newMockStorage()
	storage.err = errors.New("database connection refused")

	config := WriteBehindConfig{
		DrainInterval: 10 * time.Millisecond,
		BatchSize:     10,
		QueueCapacity: 50,
	}

	wb, err := NewWriteBehindQueue(storage, config)
	require.NoError(t, err)
	defer wb.Close()

	require.NoError(t, wb.EnqueueSet("k", "v", 0))
	wb.Flush()

	metrics := wb.Metrics()
	assert.Equal(t, int64(1), metrics.TotalErrors)
	assert.Equal(t, int64(0), metrics.TotalPersisted)
}
