package metrics

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const DefaultNamespace = "search_trends"

var ErrEmptyNamespace = errors.New("metrics namespace is required")

type Registry struct {
	registry *prometheus.Registry

	kafkaMessagesTotal     *prometheus.CounterVec
	httpRequestsTotal      *prometheus.CounterVec
	httpRequestDuration    *prometheus.HistogramVec
	aggregationQueries     prometheus.Gauge
	stopListSize           prometheus.Gauge
	snapshotItems          prometheus.Gauge
	eventsFilteredTotal    *prometheus.CounterVec
	eventsProcessingErrors *prometheus.CounterVec
}

func NewRegistry(namespace string) (*Registry, error) {
	if namespace == "" {
		return nil, ErrEmptyNamespace
	}

	registry := prometheus.NewRegistry()
	metrics := &Registry{
		registry: registry,
		kafkaMessagesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "kafka_messages_total",
			Help:      "Total number of Kafka messages processed by result",
		}, []string{"result"}),
		httpRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "http_requests_total",
			Help:      "Total number of HTTP requests by route, method and status",
		}, []string{"route", "method", "status"}),
		httpRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration in seconds by route and method",
			Buckets:   prometheus.DefBuckets,
		}, []string{"route", "method"}),
		aggregationQueries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "aggregation_queries",
			Help:      "Current number of unique queries in the aggregation window",
		}),
		stopListSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "stop_list_size",
			Help:      "Current number of queries in the stop-list",
		}),
		snapshotItems: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "snapshot_items",
			Help:      "Current number of items in the top snapshot",
		}),
		eventsFilteredTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_filtered_total",
			Help:      "Total number of filtered search events by reason",
		}, []string{"reason"}),
		eventsProcessingErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "event_processing_errors_total",
			Help:      "Total number of event processing errors by reason",
		}, []string{"reason"}),
	}

	registry.MustRegister(
		metrics.kafkaMessagesTotal,
		metrics.httpRequestsTotal,
		metrics.httpRequestDuration,
		metrics.aggregationQueries,
		metrics.stopListSize,
		metrics.snapshotItems,
		metrics.eventsFilteredTotal,
		metrics.eventsProcessingErrors,
	)

	return metrics, nil
}

func MustNewRegistry(namespace string) *Registry {
	registry, err := NewRegistry(namespace)
	if err != nil {
		panic(err)
	}
	return registry
}

func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}

func (r *Registry) Middleware(route string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(recorder, request)

		r.ObserveHTTPRequest(route, request.Method, recorder.statusCode, time.Since(startedAt))
	})
}

func (r *Registry) ObserveHTTPRequest(route, method string, statusCode int, duration time.Duration) {
	r.httpRequestsTotal.WithLabelValues(route, method, strconv.Itoa(statusCode)).Inc()
	r.httpRequestDuration.WithLabelValues(route, method).Observe(duration.Seconds())
}

func (r *Registry) IncKafkaMessage(result string) {
	r.kafkaMessagesTotal.WithLabelValues(result).Inc()
}

func (r *Registry) IncFilteredEvent(reason string) {
	r.eventsFilteredTotal.WithLabelValues(reason).Inc()
}

func (r *Registry) IncEventProcessingError(reason string) {
	r.eventsProcessingErrors.WithLabelValues(reason).Inc()
}

func (r *Registry) SetAggregationQueries(count int) {
	r.aggregationQueries.Set(float64(count))
}

func (r *Registry) SetStopListSize(count int) {
	r.stopListSize.Set(float64(count))
}

func (r *Registry) SetSnapshotItems(count int) {
	r.snapshotItems.Set(float64(count))
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}
