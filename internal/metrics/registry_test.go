package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistryHandlerExposesMetrics(t *testing.T) {
	t.Parallel()

	registry := newTestRegistry(t)
	registry.IncKafkaMessage("processed")
	registry.IncFilteredEvent("rate_limited")
	registry.IncEventProcessingError("parse_error")
	registry.SetAggregationQueries(3)
	registry.SetStopListSize(2)
	registry.SetSnapshotItems(5)

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	registry.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}

	body := response.Body.String()
	assertContains(t, body, `search_trends_kafka_messages_total{result="processed"} 1`)
	assertContains(t, body, `search_trends_events_filtered_total{reason="rate_limited"} 1`)
	assertContains(t, body, `search_trends_event_processing_errors_total{reason="parse_error"} 1`)
	assertContains(t, body, `search_trends_aggregation_queries 3`)
	assertContains(t, body, `search_trends_stop_list_size 2`)
	assertContains(t, body, `search_trends_snapshot_items 5`)
}

func TestRegistryMiddlewareRecordsHTTPMetrics(t *testing.T) {
	t.Parallel()

	registry := newTestRegistry(t)
	handler := registry.Middleware("/trends", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "ok")
	}))

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/trends", nil)

	handler.ServeHTTP(response, request)

	metricsResponse := httptest.NewRecorder()
	registry.Handler().ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	body := metricsResponse.Body.String()
	assertContains(t, body, `search_trends_http_requests_total{method="GET",route="/trends",status="202"} 1`)
	assertContains(t, body, `search_trends_http_request_duration_seconds_count{method="GET",route="/trends"} 1`)
}

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()

	registry, err := NewRegistry(DefaultNamespace)
	if err != nil {
		t.Fatalf("NewRegistry() error = %v", err)
	}

	return registry
}

func assertContains(t *testing.T, body, want string) {
	t.Helper()

	if !strings.Contains(body, want) {
		t.Fatalf("metrics body does not contain %q", want)
	}
}
