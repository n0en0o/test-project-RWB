package stoplist

import (
	"errors"
	"sort"
	"sync"

	"test-project-rwb/internal/trends"
)

var ErrEmptyQuery = errors.New("query is required")

type Store struct {
	mu    sync.RWMutex
	items map[string]struct{}
}

// NewStore создает in-memory хранилище стоп-листа
func NewStore() *Store {
	return &Store{
		items: make(map[string]struct{}),
	}
}

// Add добавляет нормализованный запрос в стоп-лист
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

// Remove удаляет нормализованный запрос из стоп-листа
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

// Contains проверяет, есть ли нормализованный запрос в стоп-листе
func (s *Store) Contains(query string) bool {
	normalized := trends.NormalizeQuery(query)
	if normalized == "" {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.items[normalized]
	return ok
}

// List возвращает отсортированную копию стоп-листа
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

// Size возвращает количество запросов в стоп-листе
func (s *Store) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items)
}
