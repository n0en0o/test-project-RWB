package trends

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizeQuery(t *testing.T) {
	t.Parallel()

	got := NormalizeQuery("  IPHONE   15\tPro  ")
	want := "iphone 15 pro"

	if got != want {
		t.Fatalf("NormalizeQuery() = %q, want %q", got, want)
	}
}

func TestNormalizeQueryLimitsLength(t *testing.T) {
	t.Parallel()

	got := NormalizeQuery(strings.Repeat("я", MaxQueryLength+10))

	if len([]rune(got)) != MaxQueryLength {
		t.Fatalf("normalized query length = %d, want %d", len([]rune(got)), MaxQueryLength)
	}
}

func TestParseSearchEvent(t *testing.T) {
	t.Parallel()

	event, err := ParseSearchEvent([]byte(`{
		"event_id": " event-1 ",
		"query": "  IPHONE   15 Pro ",
		"user_id": " user-1 ",
		"session_id": " session-1 ",
		"timestamp": "2026-05-25T12:00:00Z"
	}`))
	if err != nil {
		t.Fatalf("ParseSearchEvent() error = %v", err)
	}

	if event.EventID != "event-1" {
		t.Fatalf("EventID = %q, want %q", event.EventID, "event-1")
	}
	if event.NormalizedQuery != "iphone 15 pro" {
		t.Fatalf("NormalizedQuery = %q, want %q", event.NormalizedQuery, "iphone 15 pro")
	}
	if event.ClientID() != "user-1" {
		t.Fatalf("ClientID() = %q, want %q", event.ClientID(), "user-1")
	}
}

func TestParseSearchEventRequiresNormalizedQuery(t *testing.T) {
	t.Parallel()

	_, err := ParseSearchEvent([]byte(`{
		"event_id": "event-1",
		"query": "   ",
		"timestamp": "2026-05-25T12:00:00Z"
	}`))

	if !errors.Is(err, ErrEmptyQuery) {
		t.Fatalf("ParseSearchEvent() error = %v, want %v", err, ErrEmptyQuery)
	}
}
