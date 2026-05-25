package trends

import (
	"errors"
	"sync"
	"time"
)

const DefaultAbuseInterval = 30 * time.Second

var (
	ErrInvalidAbuseInterval = errors.New("abuse interval must be positive")
	ErrInvalidAbuseTTL      = errors.New("abuse ttl must be positive")
	ErrEventRateLimited     = errors.New("event is rate limited by anti-abuse guard")
)

type abuseKey struct {
	clientID string
	query    string
}

type AbuseGuard struct {
	mu sync.Mutex

	interval time.Duration
	ttl      time.Duration
	seen     map[abuseKey]time.Time
}

// NewAbuseGuard создает защиту от частых повторов одного запроса от одного клиента
func NewAbuseGuard(interval, ttl time.Duration) (*AbuseGuard, error) {
	if interval <= 0 {
		return nil, ErrInvalidAbuseInterval
	}
	if ttl <= 0 {
		return nil, ErrInvalidAbuseTTL
	}

	return &AbuseGuard{
		interval: interval,
		ttl:      ttl,
		seen:     make(map[abuseKey]time.Time),
	}, nil
}

// Allow проверяет, можно ли учитывать событие в агрегаторе
func (g *AbuseGuard) Allow(event SearchEvent, now time.Time) error {
	clientID := event.ClientID()
	if clientID == "" {
		return nil
	}

	query := event.NormalizedQuery
	if query == "" {
		query = NormalizeQuery(event.Query)
	}
	if query == "" {
		return ErrEmptyQuery
	}
	if event.Timestamp.IsZero() {
		return ErrEmptyTimestamp
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	g.prune(now)

	key := abuseKey{
		clientID: clientID,
		query:    query,
	}
	lastAcceptedAt, ok := g.seen[key]
	if ok && event.Timestamp.Before(lastAcceptedAt.Add(g.interval)) {
		return ErrEventRateLimited
	}

	g.seen[key] = event.Timestamp
	return nil
}

// Size возвращает количество активных ключей антинакрутки
func (g *AbuseGuard) Size(now time.Time) int {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.prune(now)
	return len(g.seen)
}

// Interval возвращает минимальный интервал между одинаковыми событиями клиента
func (g *AbuseGuard) Interval() time.Duration {
	return g.interval
}

// TTL возвращает время хранения ключей антинакрутки
func (g *AbuseGuard) TTL() time.Duration {
	return g.ttl
}

func (g *AbuseGuard) prune(now time.Time) {
	cutoff := now.Add(-g.ttl)
	for key, lastAcceptedAt := range g.seen {
		if lastAcceptedAt.Before(cutoff) {
			delete(g.seen, key)
		}
	}
}
