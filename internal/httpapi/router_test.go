package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"test-project-rwb/internal/stoplist"
	"test-project-rwb/internal/trends"
)

func TestNewRouterRegistersServiceRoutes(t *testing.T) {
	t.Parallel()

	trendsHandler := newTestTrendsHandler(t, &fakeTrendsProvider{
		maxItems: trends.MaxTopLimit,
		snapshot: trends.Snapshot{
			Items: []trends.TopItem{{Query: "iphone", Count: 1}},
		},
	})
	stopListHandler := newTestStopListHandler(t, stoplist.NewStore())
	router := NewRouter(trendsHandler, stopListHandler)

	tests := []struct {
		method string
		target string
		status int
	}{
		{method: http.MethodGet, target: "/trends", status: http.StatusOK},
		{method: http.MethodGet, target: "/stop-list", status: http.StatusOK},
		{method: http.MethodGet, target: "/healthz", status: http.StatusOK},
		{method: http.MethodGet, target: "/readyz", status: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(tt.method, tt.target, nil)

			router.ServeHTTP(response, request)

			if response.Code != tt.status {
				t.Fatalf("status = %d, want %d", response.Code, tt.status)
			}
		})
	}
}

func TestNewRouterWorksWithoutStopListHandler(t *testing.T) {
	t.Parallel()

	trendsHandler, err := NewTrendsHandler(&fakeTrendsProvider{maxItems: trends.MaxTopLimit}, 5*time.Minute)
	if err != nil {
		t.Fatalf("NewTrendsHandler() error = %v", err)
	}
	router := NewRouter(trendsHandler)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stop-list", nil)

	router.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}
