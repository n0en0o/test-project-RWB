package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"test-project-rwb/internal/trends"
)

func TestTrendsHandlerReturnsSnapshot(t *testing.T) {
	t.Parallel()

	generatedAt := time.Date(2026, 5, 25, 12, 5, 0, 0, time.UTC)
	handler := newTestTrendsHandler(t, &fakeTrendsProvider{
		maxItems: trends.MaxTopLimit,
		snapshot: trends.Snapshot{
			GeneratedAt: generatedAt,
			Items: []trends.TopItem{
				{Query: "iphone", Count: 10},
				{Query: "lego", Count: 5},
			},
		},
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/trends?limit=2", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}

	var body TrendsResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if body.WindowSeconds != 300 {
		t.Fatalf("WindowSeconds = %d, want 300", body.WindowSeconds)
	}
	if body.Limit != 2 {
		t.Fatalf("Limit = %d, want 2", body.Limit)
	}
	if body.GeneratedAt != generatedAt {
		t.Fatalf("GeneratedAt = %v, want %v", body.GeneratedAt, generatedAt)
	}
	if len(body.Items) != 2 {
		t.Fatalf("items length = %d, want 2", len(body.Items))
	}
}

func TestTrendsHandlerUsesDefaultLimit(t *testing.T) {
	t.Parallel()

	provider := &fakeTrendsProvider{
		maxItems: trends.MaxTopLimit,
		snapshot: trends.Snapshot{
			Items: []trends.TopItem{{Query: "iphone", Count: 10}},
		},
	}
	handler := newTestTrendsHandler(t, provider)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/trends", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if provider.lastLimit != DefaultTopLimit {
		t.Fatalf("provider limit = %d, want %d", provider.lastLimit, DefaultTopLimit)
	}
}

func TestTrendsHandlerRejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	tests := []string{
		"/trends?limit=bad",
		"/trends?limit=0",
		"/trends?limit=-1",
		"/trends?limit=101",
	}

	for _, target := range tests {
		t.Run(target, func(t *testing.T) {
			t.Parallel()

			handler := newTestTrendsHandler(t, &fakeTrendsProvider{maxItems: trends.MaxTopLimit})
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, target, nil)

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestTrendsHandlerRejectsUnsupportedMethod(t *testing.T) {
	t.Parallel()

	handler := newTestTrendsHandler(t, &fakeTrendsProvider{maxItems: trends.MaxTopLimit})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/trends", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}

func TestTrendsHandlerMapsProviderLimitError(t *testing.T) {
	t.Parallel()

	handler := newTestTrendsHandler(t, &fakeTrendsProvider{
		maxItems: trends.MaxTopLimit,
		err:      trends.ErrInvalidTopLimit,
	})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/trends?limit=10", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}

func TestTrendsHandlerMapsProviderUnexpectedError(t *testing.T) {
	t.Parallel()

	handler := newTestTrendsHandler(t, &fakeTrendsProvider{
		maxItems: trends.MaxTopLimit,
		err:      errors.New("storage failed"),
	})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/trends?limit=10", nil)

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
}

func newTestTrendsHandler(t *testing.T, provider TrendsProvider) *TrendsHandler {
	t.Helper()

	handler, err := NewTrendsHandler(provider, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewTrendsHandler() error = %v", err)
	}

	return handler
}

type fakeTrendsProvider struct {
	maxItems  int
	snapshot  trends.Snapshot
	err       error
	lastLimit int
}

func (p *fakeTrendsProvider) Get(limit int) (trends.Snapshot, error) {
	p.lastLimit = limit
	if p.err != nil {
		return trends.Snapshot{}, p.err
	}
	if len(p.snapshot.Items) > limit {
		p.snapshot.Items = p.snapshot.Items[:limit]
	}

	return p.snapshot, nil
}

func (p *fakeTrendsProvider) MaxItems() int {
	return p.maxItems
}
