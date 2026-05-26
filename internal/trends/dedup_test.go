package trends

import (
	"errors"
	"testing"
	"time"
)

func TestDeduplicatorDetectsDuplicateEventID(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	dedup := newTestDeduplicator(t)

	if dedup.Seen("event-1", now) {
		t.Fatalf("Seen() = true, want false")
	}
	if !dedup.Seen("event-1", now.Add(time.Second)) {
		t.Fatalf("Seen() = false, want true")
	}
}

func TestDeduplicatorForgetsOldEventID(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	dedup := newTestDeduplicator(t)

	if dedup.Seen("event-1", now) {
		t.Fatalf("Seen() = true, want false")
	}
	if dedup.Seen("event-1", now.Add(6*time.Minute)) {
		t.Fatalf("Seen() = true, want false")
	}
}

func TestDeduplicatorIgnoresEmptyEventID(t *testing.T) {
	t.Parallel()

	dedup := newTestDeduplicator(t)

	if dedup.Seen("", time.Now()) {
		t.Fatalf("Seen() = true, want false")
	}
}

func TestNewDeduplicatorRejectsInvalidTTL(t *testing.T) {
	t.Parallel()

	_, err := NewDeduplicator(0)
	if !errors.Is(err, ErrInvalidDedupTTL) {
		t.Fatalf("NewDeduplicator() error = %v, want %v", err, ErrInvalidDedupTTL)
	}
}

func newTestDeduplicator(t *testing.T) *Deduplicator {
	t.Helper()

	dedup, err := NewDeduplicator(5 * time.Minute)
	if err != nil {
		t.Fatalf("NewDeduplicator() error = %v", err)
	}
	return dedup
}
