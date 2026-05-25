package httpapi

import (
	"testing"
	"time"

	"test-project-rwb/internal/stoplist"
	"test-project-rwb/internal/trends"
)

func TestFilteredTrendsProviderFiltersStopListAndFillsLimit(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	stopList := stoplist.NewStore()
	if _, err := stopList.Add("iphone"); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	provider, err := NewFilteredTrendsProvider(&fakeTrendsProvider{
		maxItems: 4,
		snapshot: trends.Snapshot{
			GeneratedAt: generatedAt,
			Items: []trends.TopItem{
				{Query: "iphone", Count: 100},
				{Query: "lego", Count: 50},
				{Query: "airpods", Count: 40},
				{Query: "books", Count: 30},
			},
		},
	}, stopList)
	if err != nil {
		t.Fatalf("NewFilteredTrendsProvider() error = %v", err)
	}

	snapshot, err := provider.Get(2)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if snapshot.GeneratedAt != generatedAt {
		t.Fatalf("GeneratedAt = %v, want %v", snapshot.GeneratedAt, generatedAt)
	}
	if len(snapshot.Items) != 2 {
		t.Fatalf("items length = %d, want 2", len(snapshot.Items))
	}
	if snapshot.Items[0].Query != "lego" || snapshot.Items[1].Query != "airpods" {
		t.Fatalf("items = %+v, want lego and airpods", snapshot.Items)
	}
}

func TestFilteredTrendsProviderRejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	provider, err := NewFilteredTrendsProvider(&fakeTrendsProvider{maxItems: 2}, stoplist.NewStore())
	if err != nil {
		t.Fatalf("NewFilteredTrendsProvider() error = %v", err)
	}

	_, err = provider.Get(3)
	if err != trends.ErrInvalidTopLimit {
		t.Fatalf("Get() error = %v, want %v", err, trends.ErrInvalidTopLimit)
	}
}
