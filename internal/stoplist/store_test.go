package stoplist

import (
	"errors"
	"testing"
)

func TestStoreAddNormalizesQuery(t *testing.T) {
	t.Parallel()

	store := NewStore()

	normalized, err := store.Add("  IPHONE   15 Pro ")
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	if normalized != "iphone 15 pro" {
		t.Fatalf("normalized query = %q, want %q", normalized, "iphone 15 pro")
	}
	if !store.Contains("iphone   15 PRO") {
		t.Fatalf("Contains() = false, want true")
	}
}

func TestStoreRejectsEmptyQuery(t *testing.T) {
	t.Parallel()

	store := NewStore()

	_, err := store.Add("   ")
	if !errors.Is(err, ErrEmptyQuery) {
		t.Fatalf("Add() error = %v, want %v", err, ErrEmptyQuery)
	}
}

func TestStoreRemove(t *testing.T) {
	t.Parallel()

	store := NewStore()

	if _, err := store.Add("iphone"); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if _, err := store.Remove(" IPHONE "); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	if store.Contains("iphone") {
		t.Fatalf("Contains() = true, want false")
	}
}

func TestStoreContainsStopWordAsToken(t *testing.T) {
	t.Parallel()

	store := NewStore()
	if _, err := store.Add("iphone"); err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	if !store.Contains("new iphone 15 pro") {
		t.Fatalf("Contains() = false, want true")
	}
	if store.Contains("microiphone case") {
		t.Fatalf("Contains() = true, want false")
	}
}

func TestStoreListSorted(t *testing.T) {
	t.Parallel()

	store := NewStore()
	for _, query := range []string{"z query", "a query", "m query"} {
		if _, err := store.Add(query); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	items := store.List()
	want := []string{"a query", "m query", "z query"}

	if len(items) != len(want) {
		t.Fatalf("items length = %d, want %d", len(items), len(want))
	}
	for i := range want {
		if items[i] != want[i] {
			t.Fatalf("items[%d] = %q, want %q", i, items[i], want[i])
		}
	}
}
