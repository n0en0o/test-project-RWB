package trends

import (
	"errors"
	"sync"
	"time"
)

var ErrInvalidDedupTTL = errors.New("dedup ttl must be positive")

type Deduplicator struct {
	mu   sync.Mutex
	ttl  time.Duration
	seen map[string]time.Time
}

func NewDeduplicator(ttl time.Duration) (*Deduplicator, error) {
	if ttl <= 0 {
		return nil, ErrInvalidDedupTTL
	}

	return &Deduplicator{
		ttl:  ttl,
		seen: make(map[string]time.Time),
	}, nil
}

func (d *Deduplicator) Seen(eventID string, now time.Time) bool {
	if eventID == "" {
		return false
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.prune(now)

	if _, ok := d.seen[eventID]; ok {
		return true
	}
	d.seen[eventID] = now
	return false
}

func (d *Deduplicator) Size(now time.Time) int {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.prune(now)
	return len(d.seen)
}

func (d *Deduplicator) prune(now time.Time) {
	cutoff := now.Add(-d.ttl)
	for eventID, seenAt := range d.seen {
		if seenAt.Before(cutoff) {
			delete(d.seen, eventID)
		}
	}
}
