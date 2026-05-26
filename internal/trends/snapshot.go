package trends

import (
	"errors"
	"sort"
	"sync/atomic"
	"time"
)

const MaxTopLimit = 100

var ErrInvalidTopLimit = errors.New("top limit must be between 1 and max limit")

type TopItem struct {
	Query string `json:"query"`
	Count int64  `json:"count"`
}

type Snapshot struct {
	GeneratedAt time.Time `json:"generated_at"`
	Items       []TopItem `json:"items"`
}

type SnapshotStore struct {
	maxItems int
	value    atomic.Value
}

func NewSnapshotStore(maxItems int) (*SnapshotStore, error) {
	if maxItems <= 0 {
		return nil, ErrInvalidTopLimit
	}

	store := &SnapshotStore{maxItems: maxItems}
	store.value.Store(Snapshot{Items: []TopItem{}})

	return store, nil
}

func (s *SnapshotStore) Rebuild(counts map[string]int64, generatedAt time.Time) {
	items := make([]TopItem, 0, len(counts))
	for query, count := range counts {
		if count <= 0 {
			continue
		}
		items = append(items, TopItem{
			Query: query,
			Count: count,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count == items[j].Count {
			return items[i].Query < items[j].Query
		}
		return items[i].Count > items[j].Count
	})

	if len(items) > s.maxItems {
		items = items[:s.maxItems]
	}

	s.value.Store(Snapshot{
		GeneratedAt: generatedAt,
		Items:       items,
	})
}

func (s *SnapshotStore) Get(limit int) (Snapshot, error) {
	if limit <= 0 || limit > s.maxItems {
		return Snapshot{}, ErrInvalidTopLimit
	}

	snapshot, _ := s.value.Load().(Snapshot)
	items := snapshot.Items
	if len(items) > limit {
		items = items[:limit]
	}

	result := Snapshot{
		GeneratedAt: snapshot.GeneratedAt,
		Items:       make([]TopItem, len(items)),
	}
	copy(result.Items, items)

	return result, nil
}

func (s *SnapshotStore) MaxItems() int {
	return s.maxItems
}
