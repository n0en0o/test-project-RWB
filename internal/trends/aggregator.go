package trends

import (
	"errors"
	"sync"
	"time"
)

const bucketSize = time.Second

var (
	ErrInvalidWindow   = errors.New("window must be positive")
	ErrInvalidFuture   = errors.New("future skew cannot be negative")
	ErrEventTooOld     = errors.New("event is older than aggregation window")
	ErrEventFromFuture = errors.New("event is too far in the future")
)

type Aggregator struct {
	mu sync.Mutex

	window     time.Duration
	futureSkew time.Duration

	buckets map[int64]map[string]int64
	counts  map[string]int64
}

func NewAggregator(window, futureSkew time.Duration) (*Aggregator, error) {
	if window <= 0 {
		return nil, ErrInvalidWindow
	}
	if futureSkew < 0 {
		return nil, ErrInvalidFuture
	}

	return &Aggregator{
		window:     window,
		futureSkew: futureSkew,
		buckets:    make(map[int64]map[string]int64),
		counts:     make(map[string]int64),
	}, nil
}

func (a *Aggregator) Add(event SearchEvent, now time.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.validateEventTime(event.Timestamp, now); err != nil {
		return err
	}

	query := event.NormalizedQuery
	if query == "" {
		query = NormalizeQuery(event.Query)
	}
	if query == "" {
		return ErrEmptyQuery
	}

	a.prune(now)

	key := bucketKey(event.Timestamp)
	if _, ok := a.buckets[key]; !ok {
		a.buckets[key] = make(map[string]int64)
	}

	a.buckets[key][query]++
	a.counts[query]++

	return nil
}

func (a *Aggregator) Counts(now time.Time) map[string]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.prune(now)

	result := make(map[string]int64, len(a.counts))
	for query, count := range a.counts {
		result[query] = count
	}

	return result
}

func (a *Aggregator) Count(query string, now time.Time) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	normalized := NormalizeQuery(query)
	if normalized == "" {
		return 0
	}

	a.prune(now)
	return a.counts[normalized]
}

func (a *Aggregator) Window() time.Duration {
	return a.window
}

func (a *Aggregator) FutureSkew() time.Duration {
	return a.futureSkew
}

func (a *Aggregator) validateEventTime(timestamp, now time.Time) error {
	if timestamp.Before(now.Add(-a.window)) {
		return ErrEventTooOld
	}
	if timestamp.After(now.Add(a.futureSkew)) {
		return ErrEventFromFuture
	}
	return nil
}

func (a *Aggregator) prune(now time.Time) {
	cutoff := now.Add(-a.window)

	for key, bucket := range a.buckets {
		if time.Unix(key, 0).Add(bucketSize).After(cutoff) {
			continue
		}

		for query, count := range bucket {
			a.counts[query] -= count
			if a.counts[query] <= 0 {
				delete(a.counts, query)
			}
		}
		delete(a.buckets, key)
	}
}

func bucketKey(timestamp time.Time) int64 {
	return timestamp.Truncate(bucketSize).Unix()
}
