package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"test-project-rwb/internal/config"
	"test-project-rwb/internal/httpapi"
	"test-project-rwb/internal/kafka"
	"test-project-rwb/internal/metrics"
	"test-project-rwb/internal/stoplist"
	"test-project-rwb/internal/trends"
)

var ErrNotReady = errors.New("service is not ready")

func Run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	aggregator, err := trends.NewAggregator(cfg.Window, cfg.FutureSkew)
	if err != nil {
		return err
	}
	abuseGuard, err := trends.NewAbuseGuard(cfg.AbuseInterval, cfg.AbuseTTL)
	if err != nil {
		return err
	}
	deduplicator, err := trends.NewDeduplicator(cfg.Window)
	if err != nil {
		return err
	}
	snapshotStore, err := trends.NewSnapshotStore(cfg.MaxTopItems)
	if err != nil {
		return err
	}

	stopListStore := stoplist.NewStore()
	metricsRegistry, err := metrics.NewRegistry(metrics.DefaultNamespace)
	if err != nil {
		return err
	}

	processor := &eventProcessor{
		aggregator:   aggregator,
		abuseGuard:   abuseGuard,
		deduplicator: deduplicator,
		metrics:      metricsRegistry,
	}

	reader, err := kafka.NewReader(kafka.ConsumerConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   cfg.KafkaTopic,
		GroupID: cfg.KafkaGroupID,
	})
	if err != nil {
		return err
	}

	consumer, err := kafka.NewConsumer(reader, processor, logger)
	if err != nil {
		_ = reader.Close()
		return err
	}
	consumer.SetMetrics(metricsRegistry)

	filteredProvider, err := httpapi.NewFilteredTrendsProvider(snapshotStore, stopListStore)
	if err != nil {
		_ = consumer.Close()
		return err
	}
	trendsHandler, err := httpapi.NewTrendsHandler(filteredProvider, cfg.Window)
	if err != nil {
		_ = consumer.Close()
		return err
	}
	stopListHandler, err := httpapi.NewStopListHandler(stopListStore)
	if err != nil {
		_ = consumer.Close()
		return err
	}

	ready := &readiness{}
	router := httpapi.NewRouter(httpapi.RouterConfig{
		Trends:   trendsHandler,
		StopList: stopListHandler,
		Health:   httpapi.NewHealthHandler(ready),
		Metrics:  metricsRegistry,
	})

	listener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		_ = consumer.Close()
		return err
	}

	server := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 3)
	go func() {
		errCh <- consumer.Run(ctx)
	}()
	go func() {
		runSnapshotWorker(ctx, aggregator, snapshotStore, stopListStore, metricsRegistry, cfg.SnapshotInterval)
		errCh <- nil
	}()
	go func() {
		logger.Info("http server started", "addr", listener.Addr().String())
		ready.set(true)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			ready.set(false)
			_ = consumer.Close()
			return err
		}
	}

	ready.set(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = consumer.Close()
		return err
	}
	return consumer.Close()
}

type readiness struct {
	ready atomic.Bool
}

func (r *readiness) Ready(ctx context.Context) error {
	if r.ready.Load() {
		return nil
	}
	return ErrNotReady
}

func (r *readiness) set(ready bool) {
	r.ready.Store(ready)
}

type eventProcessor struct {
	aggregator   *trends.Aggregator
	abuseGuard   *trends.AbuseGuard
	deduplicator *trends.Deduplicator
	metrics      *metrics.Registry
}

func (p *eventProcessor) HandleSearchEvent(ctx context.Context, event trends.SearchEvent) error {
	now := time.Now().UTC()
	if p.deduplicator.Seen(event.EventID, now) {
		p.metrics.IncFilteredEvent("duplicate")
		return nil
	}

	if err := p.abuseGuard.Allow(event, now); err != nil {
		if errors.Is(err, trends.ErrEventRateLimited) {
			p.metrics.IncFilteredEvent("rate_limited")
			return nil
		}
		p.metrics.IncEventProcessingError("anti_abuse_error")
		return err
	}

	if err := p.aggregator.Add(event, now); err != nil {
		if errors.Is(err, trends.ErrEventTooOld) {
			p.metrics.IncFilteredEvent("too_old")
			return nil
		}
		if errors.Is(err, trends.ErrEventFromFuture) {
			p.metrics.IncFilteredEvent("from_future")
			return nil
		}
		p.metrics.IncEventProcessingError("aggregation_error")
		return err
	}

	return nil
}

func runSnapshotWorker(ctx context.Context, aggregator *trends.Aggregator, snapshotStore *trends.SnapshotStore, stopListStore *stoplist.Store, metricsRegistry *metrics.Registry, interval time.Duration) {
	rebuildSnapshot(aggregator, snapshotStore, stopListStore, metricsRegistry)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rebuildSnapshot(aggregator, snapshotStore, stopListStore, metricsRegistry)
		}
	}
}

func rebuildSnapshot(aggregator *trends.Aggregator, snapshotStore *trends.SnapshotStore, stopListStore *stoplist.Store, metricsRegistry *metrics.Registry) {
	now := time.Now().UTC()
	counts := aggregator.Counts(now)
	snapshotStore.Rebuild(counts, now)
	snapshot, err := snapshotStore.Get(snapshotStore.MaxItems())
	if err == nil {
		metricsRegistry.SetSnapshotItems(len(snapshot.Items))
	}
	metricsRegistry.SetAggregationQueries(len(counts))
	metricsRegistry.SetStopListSize(stopListStore.Size())
}
