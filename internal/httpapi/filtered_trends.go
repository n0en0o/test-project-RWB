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

func (p *FilteredTrendsProvider) MaxItems() int {
	return p.provider.MaxItems()
}
