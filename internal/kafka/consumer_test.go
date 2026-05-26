package kafka

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"test-project-rwb/internal/trends"
)

func TestConsumerRunHandlesAndCommitsValidMessage(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{
		messages: []kafkago.Message{validKafkaMessage("event-1")},
	}
	metrics := &fakeConsumerMetrics{}

	var handled []trends.SearchEvent
	consumer := newTestConsumer(t, reader, EventHandlerFunc(func(ctx context.Context, event trends.SearchEvent) error {
		handled = append(handled, event)
		return nil
	}))
	consumer.SetMetrics(metrics)

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(handled) != 1 {
		t.Fatalf("handled events = %d, want 1", len(handled))
	}
	if handled[0].EventID != "event-1" {
		t.Fatalf("EventID = %q, want %q", handled[0].EventID, "event-1")
	}
	if len(reader.committed) != 1 {
		t.Fatalf("committed messages = %d, want 1", len(reader.committed))
	}
	if metrics.kafkaMessages["processed"] != 1 {
		t.Fatalf("processed kafka messages = %d, want 1", metrics.kafkaMessages["processed"])
	}
}

func TestConsumerRunCommitsInvalidMessage(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{
		messages: []kafkago.Message{{Value: []byte(`{bad json}`)}},
	}
	metrics := &fakeConsumerMetrics{}

	consumer := newTestConsumer(t, reader, EventHandlerFunc(func(ctx context.Context, event trends.SearchEvent) error {
		t.Fatalf("handler should not be called")
		return nil
	}))
	consumer.SetMetrics(metrics)

	if err := consumer.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(reader.committed) != 1 {
		t.Fatalf("committed messages = %d, want 1", len(reader.committed))
	}
	if metrics.kafkaMessages["invalid"] != 1 {
		t.Fatalf("invalid kafka messages = %d, want 1", metrics.kafkaMessages["invalid"])
	}
	if metrics.processingErrors["parse_error"] != 1 {
		t.Fatalf("parse errors = %d, want 1", metrics.processingErrors["parse_error"])
	}
}

func TestConsumerRunDoesNotCommitWhenHandlerFails(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{
		messages: []kafkago.Message{validKafkaMessage("event-1")},
	}
	handlerErr := errors.New("handler failed")
	metrics := &fakeConsumerMetrics{}

	consumer := newTestConsumer(t, reader, EventHandlerFunc(func(ctx context.Context, event trends.SearchEvent) error {
		return handlerErr
	}))
	consumer.SetMetrics(metrics)

	err := consumer.Run(context.Background())
	if !errors.Is(err, handlerErr) {
		t.Fatalf("Run() error = %v, want %v", err, handlerErr)
	}
	if len(reader.committed) != 0 {
		t.Fatalf("committed messages = %d, want 0", len(reader.committed))
	}
	if metrics.kafkaMessages["handler_error"] != 1 {
		t.Fatalf("handler error kafka messages = %d, want 1", metrics.kafkaMessages["handler_error"])
	}
	if metrics.processingErrors["handler_error"] != 1 {
		t.Fatalf("handler errors = %d, want 1", metrics.processingErrors["handler_error"])
	}
}

func TestConsumerConfigValidate(t *testing.T) {
	t.Parallel()

	config := ConsumerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "search-events",
		GroupID: "trends-service",
	}

	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func newTestConsumer(t *testing.T, reader Reader, handler EventHandler) *Consumer {
	t.Helper()

	consumer, err := NewConsumer(reader, handler, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewConsumer() error = %v", err)
	}

	return consumer
}

func validKafkaMessage(eventID string) kafkago.Message {
	return kafkago.Message{
		Topic:     "search-events",
		Partition: 0,
		Offset:    1,
		Value: []byte(`{
			"event_id": "` + eventID + `",
			"query": "iphone 15 pro",
			"user_id": "user-1",
			"session_id": "session-1",
			"timestamp": "2026-05-25T12:00:00Z"
		}`),
	}
}

type fakeReader struct {
	messages  []kafkago.Message
	committed []kafkago.Message
	closed    bool
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	if len(r.messages) == 0 {
		return kafkago.Message{}, context.Canceled
	}

	message := r.messages[0]
	r.messages = r.messages[1:]

	return message, nil
}

func (r *fakeReader) CommitMessages(ctx context.Context, messages ...kafkago.Message) error {
	r.committed = append(r.committed, messages...)
	return nil
}

func (r *fakeReader) Close() error {
	r.closed = true
	return nil
}

var _ Reader = (*fakeReader)(nil)

type fakeConsumerMetrics struct {
	kafkaMessages    map[string]int
	processingErrors map[string]int
}

func (m *fakeConsumerMetrics) IncKafkaMessage(result string) {
	if m.kafkaMessages == nil {
		m.kafkaMessages = make(map[string]int)
	}
	m.kafkaMessages[result]++
}

func (m *fakeConsumerMetrics) IncEventProcessingError(reason string) {
	if m.processingErrors == nil {
		m.processingErrors = make(map[string]int)
	}
	m.processingErrors[reason]++
}

func TestConsumerCloseClosesReader(t *testing.T) {
	t.Parallel()

	reader := &fakeReader{}
	consumer := newTestConsumer(t, reader, EventHandlerFunc(func(ctx context.Context, event trends.SearchEvent) error {
		return nil
	}))

	if err := consumer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !reader.closed {
		t.Fatalf("reader closed = false, want true")
	}
}
