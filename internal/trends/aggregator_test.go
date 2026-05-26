package trends

import (
	"errors"
	"testing"
	"time"
)

func TestAggregatorCountsEventsInsideWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	aggregator := newTestAggregator(t)

	addEvent(t, aggregator, "event-1", "iphone 15 pro", now.Add(-4*time.Minute), now)
	addEvent(t, aggregator, "event-2", "IPHONE   15 Pro", now.Add(-3*time.Minute), now)
	addEvent(t, aggregator, "event-3", "lego", now.Add(-2*time.Minute), now)

	counts := aggregator.Counts(now)

	if counts["iphone 15 pro"] != 2 {
		t.Fatalf("iphone count = %d, want 2", counts["iphone 15 pro"])
	}
	if counts["lego"] != 1 {
		t.Fatalf("lego count = %d, want 1", counts["lego"])
	}
}

func TestAggregatorPrunesExpiredBuckets(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	aggregator := newTestAggregator(t)

	addEvent(t, aggregator, "event-1", "iphone", now.Add(-4*time.Minute), now)
	addEvent(t, aggregator, "event-2", "lego", now.Add(-1*time.Minute), now)

	later := now.Add(4 * time.Minute)
	counts := aggregator.Counts(later)

	if counts["iphone"] != 0 {
		t.Fatalf("iphone count = %d, want 0", counts["iphone"])
	}
	if counts["lego"] != 1 {
		t.Fatalf("lego count = %d, want 1", counts["lego"])
	}
}

func TestAggregatorRejectsTooOldEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	aggregator := newTestAggregator(t)

	err := aggregator.Add(SearchEvent{
		Query:           "iphone",
		NormalizedQuery: "iphone",
		Timestamp:       now.Add(-5*time.Minute - time.Second),
	}, now)

	if !errors.Is(err, ErrEventTooOld) {
		t.Fatalf("Add() error = %v, want %v", err, ErrEventTooOld)
	}
}

func TestAggregatorRejectsFarFutureEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	aggregator := newTestAggregator(t)

	err := aggregator.Add(SearchEvent{
		Query:           "iphone",
		NormalizedQuery: "iphone",
		Timestamp:       now.Add(11 * time.Second),
	}, now)

	if !errors.Is(err, ErrEventFromFuture) {
		t.Fatalf("Add() error = %v, want %v", err, ErrEventFromFuture)
	}
}

func TestAggregatorAcceptsWindowBoundaryAndFutureSkew(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	aggregator := newTestAggregator(t)

	addEvent(t, aggregator, "event-1", "boundary", now.Add(-5*time.Minute), now)
	addEvent(t, aggregator, "event-2", "future", now.Add(10*time.Second), now)

	if count := aggregator.Count("BOUNDARY", now); count != 1 {
		t.Fatalf("boundary count = %d, want 1", count)
	}
	if count := aggregator.Count("future", now); count != 1 {
		t.Fatalf("future count = %d, want 1", count)
	}
}

func TestAggregatorCountsReturnsCopy(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	aggregator := newTestAggregator(t)

	addEvent(t, aggregator, "event-1", "iphone", now, now)

	counts := aggregator.Counts(now)
	counts["iphone"] = 100

	if count := aggregator.Count("iphone", now); count != 1 {
		t.Fatalf("aggregator count = %d, want 1", count)
	}
}

func newTestAggregator(t *testing.T) *Aggregator {
	t.Helper()

	aggregator, err := NewAggregator(5*time.Minute, 10*time.Second)
	if err != nil {
		t.Fatalf("NewAggregator() error = %v", err)
	}

	return aggregator
}

func addEvent(t *testing.T, aggregator *Aggregator, eventID, query string, timestamp, now time.Time) {
	t.Helper()

	err := aggregator.Add(SearchEvent{
		EventID:         eventID,
		Query:           query,
		NormalizedQuery: NormalizeQuery(query),
		Timestamp:       timestamp,
	}, now)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
}
