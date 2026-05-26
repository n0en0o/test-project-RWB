package trends

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkAggregatorAdd(b *testing.B) {
	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	aggregator, err := NewAggregator(5*time.Minute, 10*time.Second)
	if err != nil {
		b.Fatalf("NewAggregator() error = %v", err)
	}

	events := make([]SearchEvent, 1000)
	for i := range events {
		query := "query-" + strconv.Itoa(i%100)
		events[i] = SearchEvent{
			EventID:         "event-" + strconv.Itoa(i),
			Query:           query,
			NormalizedQuery: query,
			UserID:          "user-" + strconv.Itoa(i%500),
			Timestamp:       now.Add(-time.Duration(i%300) * time.Second),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		event := events[i%len(events)]
		event.EventID = "bench-event-" + strconv.Itoa(i)
		if err := aggregator.Add(event, now); err != nil {
			b.Fatalf("Add() error = %v", err)
		}
	}
}

func BenchmarkSnapshotStoreRebuild(b *testing.B) {
	counts := make(map[string]int64, 10_000)
	for i := 0; i < 10_000; i++ {
		counts["query-"+strconv.Itoa(i)] = int64(i % 1000)
	}

	store, err := NewSnapshotStore(MaxTopLimit)
	if err != nil {
		b.Fatalf("NewSnapshotStore() error = %v", err)
	}

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Rebuild(counts, now)
	}
}

func BenchmarkSnapshotStoreGet(b *testing.B) {
	store, err := NewSnapshotStore(MaxTopLimit)
	if err != nil {
		b.Fatalf("NewSnapshotStore() error = %v", err)
	}

	counts := make(map[string]int64, 1000)
	for i := 0; i < 1000; i++ {
		counts["query-"+strconv.Itoa(i)] = int64(i)
	}
	store.Rebuild(counts, time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.Get(10); err != nil {
			b.Fatalf("Get() error = %v", err)
		}
	}
}
