package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"test-project-rwb/internal/config"
	"test-project-rwb/internal/httpapi"
	"test-project-rwb/internal/kafka"
	"test-project-rwb/internal/metrics"
	"test-project-rwb/internal/stoplist"
	"test-project-rwb/internal/trends"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("service stopped with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
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
		aggregator: aggregator,
		abuseGuard: abuseGuard,
		metrics:    metricsRegistry,
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
	healthHandler := httpapi.NewHealthHandler(nil)

	mux := http.NewServeMux()
	mux.Handle("/trends", metricsRegistry.Middleware("/trends", trendsHandler))
	mux.Handle("/stop-list", metricsRegistry.Middleware("/stop-list", stopListHandler))
	mux.Handle("/stop-list/", metricsRegistry.Middleware("/stop-list/{query}", stopListHandler))
	mux.Handle("/healthz", metricsRegistry.Middleware("/healthz", healthHandler))
	mux.Handle("/readyz", metricsRegistry.Middleware("/readyz", healthHandler))
	mux.Handle("/metrics", metricsRegistry.Handler())

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 3)
	go func() {
		errCh <- consumer.Run(ctx)
	}()
	go func() {
		runSnapshotWorker(ctx, aggregator, snapshotStore, stopListStore, metricsRegistry, cfg.SnapshotInterval)
		errCh <- nil
	}()
	go func() {
		logger.Info("http server started", "addr", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			stop()
			_ = consumer.Close()
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		_ = consumer.Close()
		return err
	}
	return consumer.Close()
}

type eventProcessor struct {
	aggregator *trends.Aggregator
	abuseGuard *trends.AbuseGuard
	metrics    *metrics.Registry
}

func (p *eventProcessor) HandleSearchEvent(ctx context.Context, event trends.SearchEvent) error {
	now := time.Now().UTC()
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
