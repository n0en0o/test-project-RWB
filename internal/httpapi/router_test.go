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
	router := NewRouter(RouterConfig{
		Trends:   trendsHandler,
		StopList: stopListHandler,
	})

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
	router := NewRouter(RouterConfig{Trends: trendsHandler})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/stop-list", nil)

	router.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestNewRouterRegistersMetrics(t *testing.T) {
	t.Parallel()

	trendsHandler := newTestTrendsHandler(t, &fakeTrendsProvider{maxItems: trends.MaxTopLimit})
	metrics := &fakeRouterMetrics{}
	router := NewRouter(RouterConfig{
		Trends:  trendsHandler,
		Metrics: metrics,
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	router.ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusAccepted)
	}
	if metrics.routes["/trends"] != 1 {
		t.Fatalf("route metrics = %d, want 1", metrics.routes["/trends"])
	}
}

type fakeRouterMetrics struct {
	routes map[string]int
}

func (m *fakeRouterMetrics) Middleware(route string, next http.Handler) http.Handler {
	if m.routes == nil {
		m.routes = make(map[string]int)
	}
	m.routes[route]++
	return next
}

func (m *fakeRouterMetrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
}
