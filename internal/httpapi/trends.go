package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"test-project-rwb/internal/trends"
)

const DefaultTopLimit = 10

var ErrNilTrendsProvider = errors.New("trends provider is required")

type TrendsProvider interface {
	Get(limit int) (trends.Snapshot, error)
	MaxItems() int
}

type TrendsHandler struct {
	provider     TrendsProvider
	window       time.Duration
	defaultLimit int
}

type TrendsResponse struct {
	WindowSeconds int64            `json:"window_seconds"`
	Limit         int              `json:"limit"`
	GeneratedAt   time.Time        `json:"generated_at"`
	Items         []trends.TopItem `json:"items"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

// NewTrendsHandler создает HTTP handler для выдачи поискового топа
func NewTrendsHandler(provider TrendsProvider, window time.Duration) (*TrendsHandler, error) {
	if provider == nil {
		return nil, ErrNilTrendsProvider
	}

	return &TrendsHandler{
		provider:     provider,
		window:       window,
		defaultLimit: DefaultTopLimit,
	}, nil
}

// NewRouter создает HTTP router сервиса
func NewRouter(trendsHandler *TrendsHandler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/trends", trendsHandler)

	return mux
}

func (h *TrendsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{Error: "method not allowed"})
		return
	}

	limit, err := h.parseLimit(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: err.Error()})
		return
	}

	snapshot, err := h.provider.Get(limit)
	if err != nil {
		if errors.Is(err, trends.ErrInvalidTopLimit) {
			writeJSON(w, http.StatusBadRequest, ErrorResponse{Error: "limit is out of range"})
			return
		}
		writeJSON(w, http.StatusInternalServerError, ErrorResponse{Error: "failed to read trends"})
		return
	}

	writeJSON(w, http.StatusOK, TrendsResponse{
		WindowSeconds: int64(h.window.Seconds()),
		Limit:         limit,
		GeneratedAt:   snapshot.GeneratedAt,
		Items:         snapshot.Items,
	})
}

func (h *TrendsHandler) parseLimit(r *http.Request) (int, error) {
	rawLimit := r.URL.Query().Get("limit")
	if rawLimit == "" {
		return h.defaultLimit, nil
	}

	limit, err := strconv.Atoi(rawLimit)
	if err != nil {
		return 0, errors.New("limit must be an integer")
	}
	if limit <= 0 {
		return 0, errors.New("limit must be positive")
	}
	if limit > h.provider.MaxItems() {
		return 0, errors.New("limit is out of range")
	}

	return limit, nil
}

func writeJSON(w http.ResponseWriter, statusCode int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(value)
}
