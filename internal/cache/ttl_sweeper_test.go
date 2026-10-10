package cache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"distributed-cache-service/internal/cache"
	"distributed-cache-service/internal/store"
)

func TestTTLSweeper_ActiveEviction(t *testing.T) {
	s1 := store.New()
	s2 := store.New()

	// Insert items into shard 1: 10 expired, 10 valid
	for i := 0; i < 10; i++ {
		s1.Set(fmt.Sprintf("exp-%d", i), "val", 10*time.Millisecond)
		s1.Set(fmt.Sprintf("valid-%d", i), "val", 10*time.Minute)
	}

	// Insert items into shard 2: 10 expired, 5 valid
	for i := 0; i < 10; i++ {
		s2.Set(fmt.Sprintf("exp-%d", i), "val", 10*time.Millisecond)
	}
	for i := 0; i < 5; i++ {
		s2.Set(fmt.Sprintf("valid-%d", i), "val", 10*time.Minute)
	}

	// Wait for TTL expiry
	time.Sleep(25 * time.Millisecond)

	sweeper := cache.NewTTLSweeper(
		[]cache.Shard{s1, s2},
		cache.WithInterval(10*time.Millisecond),
		cache.WithSampleSize(50),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sweeper.Start(ctx)
	if !sweeper.IsRunning() {
		t.Fatalf("expected sweeper to be running")
	}

	// Wait for ticker to run a few iterations
	time.Sleep(50 * time.Millisecond)

	sweeper.Stop()
	if sweeper.IsRunning() {
		t.Fatalf("expected sweeper to be stopped")
	}

	if sweeper.TotalEvicted() != 20 {
		t.Errorf("expected 20 evicted keys, got %d", sweeper.TotalEvicted())
	}

	// Verify valid items still exist
	for i := 0; i < 10; i++ {
		if _, found := s1.Get(fmt.Sprintf("valid-%d", i)); !found {
			t.Errorf("expected valid key valid-%d to remain in s1", i)
		}
	}
}

func TestTTLSweeper_GracefulShutdown_Stop(t *testing.T) {
	s := store.New()
	sweeper := cache.NewTTLSweeper(
		[]cache.Shard{s},
		cache.WithInterval(10*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sweeper.Start(ctx)
	if !sweeper.IsRunning() {
		t.Fatal("sweeper should be running")
	}

	sweeper.Stop()
	if sweeper.IsRunning() {
		t.Fatal("sweeper should have stopped")
	}

	// Calling Stop again should be a safe no-op
	sweeper.Stop()
}

func TestTTLSweeper_GracefulShutdown_ContextCancel(t *testing.T) {
	s := store.New()
	sweeper := cache.NewTTLSweeper(
		[]cache.Shard{s},
		cache.WithInterval(10*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	sweeper.Start(ctx)

	if !sweeper.IsRunning() {
		t.Fatal("sweeper should be running")
	}

	cancel() // Cancel context

	// Give goroutine time to exit
	time.Sleep(30 * time.Millisecond)

	if sweeper.IsRunning() {
		t.Fatal("sweeper should have stopped after context cancellation")
	}
}

func TestTTLSweeper_MultipleShardsSampling(t *testing.T) {
	shards := make([]cache.Shard, 3)
	stores := make([]*store.Store, 3)

	for i := 0; i < 3; i++ {
		st := store.New()
		stores[i] = st
		shards[i] = st

		for j := 0; j < 15; j++ {
			st.Set(fmt.Sprintf("k-%d-%d", i, j), "val", 5*time.Millisecond)
		}
	}

	time.Sleep(15 * time.Millisecond)

	sweeper := cache.NewTTLSweeper(
		shards,
		cache.WithInterval(10*time.Millisecond),
		cache.WithSampleSize(20),
	)

	evicted := sweeper.SweepOnce()
	if evicted != 45 {
		t.Errorf("expected 45 keys evicted in one sweep round, got %d", evicted)
	}

	if sweeper.TotalEvicted() != 45 {
		t.Errorf("expected TotalEvicted to be 45, got %d", sweeper.TotalEvicted())
	}
}
