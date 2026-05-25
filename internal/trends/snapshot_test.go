package trends

import (
	"errors"
	"testing"
	"time"
)

func TestSnapshotStoreRebuildSortsByCountAndQuery(t *testing.T) {
	t.Parallel()

	store := newTestSnapshotStore(t, MaxTopLimit)
	generatedAt := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)

	store.Rebuild(map[string]int64{
		"lego":          5,
		"iphone":        10,
		"airpods":       10,
		"empty ignored": 0,
	}, generatedAt)

	snapshot, err := store.Get(3)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	want := []TopItem{
		{Query: "airpods", Count: 10},
		{Query: "iphone", Count: 10},
		{Query: "lego", Count: 5},
	}

	if snapshot.GeneratedAt != generatedAt {
		t.Fatalf("GeneratedAt = %v, want %v", snapshot.GeneratedAt, generatedAt)
	}
	assertTopItems(t, snapshot.Items, want)
}

func TestSnapshotStoreLimitsItems(t *testing.T) {
	t.Parallel()

	store := newTestSnapshotStore(t, 2)

	store.Rebuild(map[string]int64{
		"first":  3,
		"second": 2,
		"third":  1,
	}, time.Now())

	snapshot, err := store.Get(2)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	assertTopItems(t, snapshot.Items, []TopItem{
		{Query: "first", Count: 3},
		{Query: "second", Count: 2},
	})
}

func TestSnapshotStoreGetReturnsCopy(t *testing.T) {
	t.Parallel()

	store := newTestSnapshotStore(t, MaxTopLimit)
	store.Rebuild(map[string]int64{"iphone": 1}, time.Now())

	first, err := store.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	first.Items[0].Query = "changed"

	second, err := store.Get(1)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if second.Items[0].Query != "iphone" {
		t.Fatalf("snapshot item query = %q, want %q", second.Items[0].Query, "iphone")
	}
}

func TestSnapshotStoreRejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	store := newTestSnapshotStore(t, MaxTopLimit)

	_, err := store.Get(MaxTopLimit + 1)
	if !errors.Is(err, ErrInvalidTopLimit) {
		t.Fatalf("Get() error = %v, want %v", err, ErrInvalidTopLimit)
	}
}

func newTestSnapshotStore(t *testing.T, maxItems int) *SnapshotStore {
	t.Helper()

	store, err := NewSnapshotStore(maxItems)
	if err != nil {
		t.Fatalf("NewSnapshotStore() error = %v", err)
	}

	return store
}

func assertTopItems(t *testing.T, got, want []TopItem) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("items length = %d, want %d", len(got), len(want))
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("items[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}
