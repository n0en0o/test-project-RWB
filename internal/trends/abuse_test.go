package trends

import (
	"errors"
	"testing"
	"time"
)

func TestAbuseGuardRateLimitsSameClientAndQuery(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	guard := newTestAbuseGuard(t)

	first := testSearchEvent("event-1", "iphone", "user-1", "", now)
	second := testSearchEvent("event-2", "IPHONE", "user-1", "", now.Add(10*time.Second))

	if err := guard.Allow(first, now); err != nil {
		t.Fatalf("first Allow() error = %v", err)
	}

	err := guard.Allow(second, now.Add(10*time.Second))
	if !errors.Is(err, ErrEventRateLimited) {
		t.Fatalf("second Allow() error = %v, want %v", err, ErrEventRateLimited)
	}
}

func TestAbuseGuardAllowsSameClientAndQueryAfterInterval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	guard := newTestAbuseGuard(t)

	first := testSearchEvent("event-1", "iphone", "user-1", "", now)
	second := testSearchEvent("event-2", "iphone", "user-1", "", now.Add(DefaultAbuseInterval))

	if err := guard.Allow(first, now); err != nil {
		t.Fatalf("first Allow() error = %v", err)
	}
	if err := guard.Allow(second, now.Add(DefaultAbuseInterval)); err != nil {
		t.Fatalf("second Allow() error = %v", err)
	}
}

func TestAbuseGuardAllowsDifferentClientOrQuery(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	guard := newTestAbuseGuard(t)

	events := []SearchEvent{
		testSearchEvent("event-1", "iphone", "user-1", "", now),
		testSearchEvent("event-2", "iphone", "user-2", "", now.Add(10*time.Second)),
		testSearchEvent("event-3", "lego", "user-1", "", now.Add(10*time.Second)),
	}

	for _, event := range events {
		if err := guard.Allow(event, event.Timestamp); err != nil {
			t.Fatalf("Allow(%s) error = %v", event.EventID, err)
		}
	}
}

func TestAbuseGuardUsesSessionIDWhenUserIDIsEmpty(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	guard := newTestAbuseGuard(t)

	first := testSearchEvent("event-1", "iphone", "", "session-1", now)
	second := testSearchEvent("event-2", "iphone", "", "session-1", now.Add(10*time.Second))

	if err := guard.Allow(first, now); err != nil {
		t.Fatalf("first Allow() error = %v", err)
	}

	err := guard.Allow(second, now.Add(10*time.Second))
	if !errors.Is(err, ErrEventRateLimited) {
		t.Fatalf("second Allow() error = %v, want %v", err, ErrEventRateLimited)
	}
}

func TestAbuseGuardAllowsAnonymousEvents(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	guard := newTestAbuseGuard(t)

	first := testSearchEvent("event-1", "iphone", "", "", now)
	second := testSearchEvent("event-2", "iphone", "", "", now.Add(time.Second))

	if err := guard.Allow(first, now); err != nil {
		t.Fatalf("first Allow() error = %v", err)
	}
	if err := guard.Allow(second, now.Add(time.Second)); err != nil {
		t.Fatalf("second Allow() error = %v", err)
	}
}

func TestAbuseGuardPrunesOldKeys(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	guard := newTestAbuseGuard(t)

	event := testSearchEvent("event-1", "iphone", "user-1", "", now)
	if err := guard.Allow(event, now); err != nil {
		t.Fatalf("Allow() error = %v", err)
	}

	if size := guard.Size(now.Add(6 * time.Minute)); size != 0 {
		t.Fatalf("Size() = %d, want 0", size)
	}
}

func newTestAbuseGuard(t *testing.T) *AbuseGuard {
	t.Helper()

	guard, err := NewAbuseGuard(DefaultAbuseInterval, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewAbuseGuard() error = %v", err)
	}

	return guard
}

func testSearchEvent(eventID, query, userID, sessionID string, timestamp time.Time) SearchEvent {
	return SearchEvent{
		EventID:         eventID,
		Query:           query,
		UserID:          userID,
		SessionID:       sessionID,
		Timestamp:       timestamp,
		NormalizedQuery: NormalizeQuery(query),
	}
}
