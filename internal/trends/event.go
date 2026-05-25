package trends

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

// MaxQueryLength задает максимальную длину нормализованного поискового запроса
const MaxQueryLength = 256

var (
	ErrEmptyEventID   = errors.New("event_id is required")
	ErrEmptyQuery     = errors.New("query is required")
	ErrEmptyTimestamp = errors.New("timestamp is required")
)

// SearchEvent описывает контракт сообщения, которое сервис читает из Kafka
type SearchEvent struct {
	EventID         string    `json:"event_id"`
	Query           string    `json:"query"`
	UserID          string    `json:"user_id,omitempty"`
	SessionID       string    `json:"session_id,omitempty"`
	Timestamp       time.Time `json:"timestamp"`
	NormalizedQuery string    `json:"-"`
}

func ParseSearchEvent(payload []byte) (SearchEvent, error) {
	var event SearchEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return SearchEvent{}, err
	}

	event.EventID = strings.TrimSpace(event.EventID)
	event.UserID = strings.TrimSpace(event.UserID)
	event.SessionID = strings.TrimSpace(event.SessionID)
	event.NormalizedQuery = NormalizeQuery(event.Query)

	if event.EventID == "" {
		return SearchEvent{}, ErrEmptyEventID
	}
	if event.NormalizedQuery == "" {
		return SearchEvent{}, ErrEmptyQuery
	}
	if event.Timestamp.IsZero() {
		return SearchEvent{}, ErrEmptyTimestamp
	}

	return event, nil
}

// NormalizeQuery приводит поисковую строку к форме, пригодной для агрегации
func NormalizeQuery(query string) string {
	normalized := strings.ToLower(strings.Join(strings.Fields(query), " "))
	if utf8.RuneCountInString(normalized) <= MaxQueryLength {
		return normalized
	}

	runes := []rune(normalized)
	return string(runes[:MaxQueryLength])
}

// ClientID возвращает идентификатор клиента для базовой антинакрутки
func (e SearchEvent) ClientID() string {
	if e.UserID != "" {
		return e.UserID
	}
	return e.SessionID
}
