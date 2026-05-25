package httpapi

import (
	"errors"

	"test-project-rwb/internal/trends"
)

var ErrNilTrendsFilter = errors.New("trends filter is required")

type QueryFilter interface {
	Contains(query string) bool
}

type FilteredTrendsProvider struct {
	provider TrendsProvider
	filter   QueryFilter
}

// NewFilteredTrendsProvider создает provider, скрывающий запросы из стоп-листа
func NewFilteredTrendsProvider(provider TrendsProvider, filter QueryFilter) (*FilteredTrendsProvider, error) {
	if provider == nil {
		return nil, ErrNilTrendsProvider
	}
	if filter == nil {
		return nil, ErrNilTrendsFilter
	}

	return &FilteredTrendsProvider{
		provider: provider,
		filter:   filter,
	}, nil
}

// Get возвращает top limit после фильтрации стоп-листа
func (p *FilteredTrendsProvider) Get(limit int) (trends.Snapshot, error) {
	if limit <= 0 || limit > p.MaxItems() {
		return trends.Snapshot{}, trends.ErrInvalidTopLimit
	}

	snapshot, err := p.provider.Get(p.provider.MaxItems())
	if err != nil {
		return trends.Snapshot{}, err
	}

	items := make([]trends.TopItem, 0, limit)
	for _, item := range snapshot.Items {
		if p.filter.Contains(item.Query) {
			continue
		}

		items = append(items, item)
		if len(items) == limit {
			break
		}
	}

	return trends.Snapshot{
		GeneratedAt: snapshot.GeneratedAt,
		Items:       items,
	}, nil
}

// MaxItems возвращает максимальный размер top provider
func (p *FilteredTrendsProvider) MaxItems() int {
	return p.provider.MaxItems()
}
