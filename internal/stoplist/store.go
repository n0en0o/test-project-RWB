package stoplist

import (
	"errors"
	"sort"
	"strings"
	"sync"

	"test-project-rwb/internal/trends"
)

var ErrEmptyQuery = errors.New("query is required")

type Store struct {
	mu    sync.RWMutex
	items map[string]struct{}
}

func NewStore() *Store {
	return &Store{
		items: make(map[string]struct{}),
	}
}

func (s *Store) Add(query string) (string, error) {
	normalized := trends.NormalizeQuery(query)
	if normalized == "" {
		return "", ErrEmptyQuery
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[normalized] = struct{}{}
	return normalized, nil
}

func (s *Store) Remove(query string) (string, error) {
	normalized := trends.NormalizeQuery(query)
	if normalized == "" {
		return "", ErrEmptyQuery
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.items, normalized)
	return normalized, nil
}

func (s *Store) Contains(query string) bool {
	normalized := trends.NormalizeQuery(query)
	if normalized == "" {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.items[normalized]; ok {
		return true
	}
	for _, token := range strings.Fields(normalized) {
		if _, ok := s.items[token]; ok {
			return true
		}
	}
	return false
}

func (s *Store) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]string, 0, len(s.items))
	for query := range s.items {
		items = append(items, query)
	}
	sort.Strings(items)

	return items
}

func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items)
}
