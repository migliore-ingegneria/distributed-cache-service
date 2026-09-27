package sharding

import (
	"context"
	"strconv"
	"testing"
)

func TestMap_Add(t *testing.T) {
	m := New(3, nil)
	m.Add("node1", "node2")

	key := "my_key"
	node := m.Get(key)
	if node == "" {
		t.Fatal("expected to get a node")
	}
}

func TestMap_Consistency(t *testing.T) {
	m := New(3, nil)
	m.Add("node1", "node2", "node3")

	key := "stable_key"
	node1 := m.Get(key)

	// Adding a new node shouldn't change the mapping for MOST keys,
	// but for a single key it might.
	// However, if we remove the node we got, we should get another one.
	// Let's test stability: call Get multiple times
	if m.Get(key) != node1 {
		t.Fatalf("hashing should be consistent")
	}
}

func TestMap_DistributionSkew(t *testing.T) {
	// Test with 1 virtual node (High Skew expected)
	m1 := New(1, nil)
	nodes := []string{"node1", "node2", "node3", "node4", "node5"}
	m1.Add(nodes...)

	// Note: Virtual Nodes are NOT Replicas.
	// - Virtual Nodes: Improve distribution (Consistent Hashing).
	// - Replicas (Raft): Provide Fault Tolerance (Redundancy).
	// Here we test distribution logic only.

	counts1 := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := "key_" + strconv.Itoa(i)
		node := m1.Get(key)
		counts1[node]++
	}

	// Test with 100 virtual nodes (Low Skew expected)
	m100 := New(100, nil)
	m100.Add(nodes...)

	counts100 := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := "key_" + strconv.Itoa(i)
		node := m100.Get(key)
		counts100[node]++
	}

	// Calculate Standard Deviation for both
	stdDev1 := calculateStdDev(counts1, 1000, len(nodes))
	stdDev100 := calculateStdDev(counts100, 1000, len(nodes))

	t.Logf("StdDev (1 vNode): %.2f", stdDev1)
	t.Logf("StdDev (100 vNodes): %.2f", stdDev100)

	if stdDev100 >= stdDev1 {
		t.Errorf("Expected 100 vNodes to have lower skew (stdDev %.2f) than 1 vNode (stdDev %.2f)", stdDev100, stdDev1)
	}
}

func calculateStdDev(counts map[string]int, total, n int) float64 {
	mean := float64(total) / float64(n)
	var sumSquares float64
	for _, count := range counts {
		diff := float64(count) - mean
		sumSquares += diff * diff
	}
	// Add 0s for nodes that got no keys
	missing := n - len(counts)
	for i := 0; i < missing; i++ {
		diff := 0 - mean
		sumSquares += diff * diff
	}
	return (sumSquares / float64(n)) // Simplified variance (not sqrt for comparison but named stddev for clarity)
}

func TestDefaultVirtualNodes(t *testing.T) {
	m := New(0, nil)
	if m.virtualNodes != DefaultVirtualNodes {
		t.Errorf("Expected default virtual nodes to be %d, got %d", DefaultVirtualNodes, m.virtualNodes)
	}
}

func TestCalculateRebalancePlan_Graceful1OverN(t *testing.T) {
	currentRing := New(256, nil)
	currentRing.Add("node1", "node2", "node3")

	targetRing := New(256, nil)
	targetRing.Add("node1", "node2", "node3", "node4") // node4 joins

	var keys []string
	for i := 0; i < 1000; i++ {
		keys = append(keys, "cache_key_"+strconv.Itoa(i))
	}

	plan, err := currentRing.CalculateRebalancePlan(context.Background(), keys, targetRing)
	if err != nil {
		t.Fatalf("unexpected rebalance error: %v", err)
	}

	if plan.TotalKeys != 1000 {
		t.Errorf("expected 1000 total keys, got %d", plan.TotalKeys)
	}

	// Adding a 4th node (3 -> 4) should ideally move ~1/4 (25%) of total keys to node4
	migratedRatio := float64(len(plan.MigratedKeys)) / float64(plan.TotalKeys)
	t.Logf("Migrated keys ratio upon 4th node join: %.2f%% (%d/1000)", migratedRatio*100, len(plan.MigratedKeys))

	if migratedRatio < 0.15 || migratedRatio > 0.35 {
		t.Errorf("expected ~25%% key migration ratio, got %.2f%%", migratedRatio*100)
	}

	// Assert all migrated keys are targeting node4
	for _, m := range plan.MigratedKeys {
		if m.Target != "node4" {
			t.Errorf("expected migrated key target to be node4, got %s", m.Target)
		}
	}
}

func TestCalculateRebalancePlan_ContextCancellation(t *testing.T) {
	currentRing := New(256, nil)
	currentRing.Add("node1", "node2")

	targetRing := New(256, nil)
	targetRing.Add("node1", "node2", "node3")

	var keys []string
	for i := 0; i < 1000; i++ {
		keys = append(keys, "cache_key_"+strconv.Itoa(i))
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel context immediately

	_, err := currentRing.CalculateRebalancePlan(ctx, keys, targetRing)
	if err == nil {
		t.Errorf("expected error on cancelled context, got nil")
	}
}
