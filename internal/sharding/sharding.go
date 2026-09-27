package sharding

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// DefaultVirtualNodes defines the standard 256 vnodes per physical node topology.
const DefaultVirtualNodes = 256

// ErrContextCancelled is returned when key rebalance calculation is interrupted by context cancellation.
var ErrContextCancelled = errors.New("rebalance operation cancelled by context")

// Hash maps bytes to uint32
type Hash func(data []byte) uint32

// Map contains all hashed keys
type Map struct {
	hash         Hash
	virtualNodes int
	keys         []int // Sorted
	hashMap      map[int]string
	mu           sync.RWMutex
}

// KeyMigration describes a single key transfer between node topologies.
type KeyMigration struct {
	Key    string
	Source string
	Target string
}

// RebalancePlan represents the planned key transfers for node join/leave events.
type RebalancePlan struct {
	MigratedKeys []KeyMigration
	TotalKeys    int
	UnmovedKeys  int
}

// New creates a new Map object. Defaults to 256 vnodes if virtualNodes <= 0.
func New(virtualNodes int, fn Hash) *Map {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	m := &Map{
		virtualNodes: virtualNodes,
		hash:         fn,
		hashMap:      make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		for i := 0; i < m.virtualNodes; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.keys) == 0 {
		return ""
	}

	hash := int(m.hash([]byte(key)))

	// Binary search for appropriate replica
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})

	// If we have gone past the end, go back to the start
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}

// Remove removes a key from the hash.
func (m *Map) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create a new hashMap without the removed key
	newHashMap := make(map[int]string)
	var newKeys []int

	for k, v := range m.hashMap {
		if v != key {
			newHashMap[k] = v
			newKeys = append(newKeys, k)
		}
	}

	m.hashMap = newHashMap
	m.keys = newKeys
	sort.Ints(m.keys)
}

// CalculateRebalancePlan computes the minimal key migration set from current map `m` to targetMap.
// Monitors ctx.Done() to gracefully cancel long-running rebalance calculations.
func (m *Map) CalculateRebalancePlan(ctx context.Context, allKeys []string, targetMap *Map) (*RebalancePlan, error) {
	plan := &RebalancePlan{
		TotalKeys: len(allKeys),
	}

	for idx, key := range allKeys {
		// Periodically check context cancellation to prevent memory leaks / blocking
		if idx%64 == 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: %v", ErrContextCancelled, ctx.Err())
			default:
			}
		}

		currentHost := m.Get(key)
		targetHost := targetMap.Get(key)

		if currentHost != targetHost && targetHost != "" {
			plan.MigratedKeys = append(plan.MigratedKeys, KeyMigration{
				Key:    key,
				Source: currentHost,
				Target: targetHost,
			})
		} else {
			plan.UnmovedKeys++
		}
	}

	return plan, nil
}
