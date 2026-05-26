package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"test-project-rwb/internal/metrics"
	"test-project-rwb/internal/trends"
)

func TestReadinessFlag(t *testing.T) {
	t.Parallel()

	ready := &readiness{}
	if err := ready.Ready(context.Background()); !errors.Is(err, ErrNotReady) {
		t.Fatalf("Ready() error = %v, want %v", err, ErrNotReady)
	}

	ready.set(true)
	if err := ready.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
}

func TestEventProcessorSkipsDuplicateEvents(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	aggregator, err := trends.NewAggregator(5*time.Minute, 10*time.Second)
	if err != nil {
		t.Fatalf("NewAggregator() error = %v", err)
	}
	abuseGuard, err := trends.NewAbuseGuard(30*time.Second, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewAbuseGuard() error = %v", err)
	}
	deduplicator, err := trends.NewDeduplicator(5 * time.Minute)
	if err != nil {
		t.Fatalf("NewDeduplicator() error = %v", err)
	}
	registry, err := metrics.NewRegistry(metrics.DefaultNamespace)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}

	processor := &eventProcessor{
		aggregator:   aggregator,
		abuseGuard:   abuseGuard,
		deduplicator: deduplicator,
		metrics:      registry,
	}
	event := trends.SearchEvent{
		EventID:         "event-1",
		Query:           "iphone",
		NormalizedQuery: "iphone",
		UserID:          "user-1",
		Timestamp:       now,
	}

	if err := processor.HandleSearchEvent(context.Background(), event); err != nil {
		t.Fatalf("HandleSearchEvent() error = %v", err)
	}
	if err := processor.HandleSearchEvent(context.Background(), event); err != nil {
		t.Fatalf("HandleSearchEvent() error = %v", err)
	}

	if count := aggregator.Count("iphone", now); count != 1 {
		t.Fatalf("aggregated count = %d, want 1", count)
	}
}
