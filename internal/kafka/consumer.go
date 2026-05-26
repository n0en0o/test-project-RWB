package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"test-project-rwb/internal/trends"
)

const (
	defaultMinBytes = 1
	defaultMaxBytes = 10e6
	defaultMaxWait  = time.Second
)

var (
	ErrEmptyBrokers = errors.New("kafka brokers are required")
	ErrEmptyTopic   = errors.New("kafka topic is required")
	ErrEmptyGroupID = errors.New("kafka group id is required")
	ErrNilReader    = errors.New("kafka reader is required")
	ErrNilHandler   = errors.New("kafka event handler is required")
)

type ConsumerConfig struct {
	Brokers  []string
	Topic    string
	GroupID  string
	MinBytes int
	MaxBytes int
	MaxWait  time.Duration
}

type Reader interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, messages ...kafkago.Message) error
	Close() error
}

type EventHandler interface {
	HandleSearchEvent(ctx context.Context, event trends.SearchEvent) error
}

type EventHandlerFunc func(ctx context.Context, event trends.SearchEvent) error

func (f EventHandlerFunc) HandleSearchEvent(ctx context.Context, event trends.SearchEvent) error {
	return f(ctx, event)
}

type ConsumerMetrics interface {
	IncKafkaMessage(result string)
	IncEventProcessingError(reason string)
}

type Consumer struct {
	reader  Reader
	handler EventHandler
	logger  *slog.Logger
	metrics ConsumerMetrics
}

// NewReader создает Kafka reader из конфигурации consumer
func NewReader(config ConsumerConfig) (*kafkago.Reader, error) {
	config = config.withDefaults()
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  config.Brokers,
		Topic:    config.Topic,
		GroupID:  config.GroupID,
		MinBytes: config.MinBytes,
		MaxBytes: config.MaxBytes,
		MaxWait:  config.MaxWait,
	}), nil
}

// NewConsumer создает consumer для чтения поисковых событий из Kafka
func NewConsumer(reader Reader, handler EventHandler, logger *slog.Logger) (*Consumer, error) {
	if reader == nil {
		return nil, ErrNilReader
	}
	if handler == nil {
		return nil, ErrNilHandler
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &Consumer{
		reader:  reader,
		handler: handler,
		logger:  logger,
	}, nil
}

// SetMetrics подключает observer для Kafka consumer метрик
func (c *Consumer) SetMetrics(metrics ConsumerMetrics) {
	c.metrics = metrics
}

// Run читает сообщения до отмены контекста
func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("fetch kafka message: %w", err)
		}

		if err := c.handleMessage(ctx, message); err != nil {
			return err
		}
	}
}

// Close закрывает reader consumer
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// Validate проверяет обязательные параметры Kafka consumer
func (c ConsumerConfig) Validate() error {
	if len(c.Brokers) == 0 {
		return ErrEmptyBrokers
	}
	if c.Topic == "" {
		return ErrEmptyTopic
	}
	if c.GroupID == "" {
		return ErrEmptyGroupID
	}
	return nil
}

func (c ConsumerConfig) withDefaults() ConsumerConfig {
	if c.MinBytes == 0 {
		c.MinBytes = defaultMinBytes
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = defaultMaxBytes
	}
	if c.MaxWait == 0 {
		c.MaxWait = defaultMaxWait
	}
	return c
}

func (c *Consumer) handleMessage(ctx context.Context, message kafkago.Message) error {
	event, err := trends.ParseSearchEvent(message.Value)
	if err != nil {
		c.recordKafkaMessage("invalid")
		c.recordEventProcessingError("parse_error")
		c.logger.WarnContext(ctx, "skip invalid kafka message", "topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			c.recordKafkaMessage("commit_error")
			return fmt.Errorf("commit invalid kafka message: %w", err)
		}
		return nil
	}

	if err := c.handler.HandleSearchEvent(ctx, event); err != nil {
		c.recordKafkaMessage("handler_error")
		c.recordEventProcessingError("handler_error")
		return fmt.Errorf("handle search event: %w", err)
	}

	if err := c.reader.CommitMessages(ctx, message); err != nil {
		c.recordKafkaMessage("commit_error")
		return fmt.Errorf("commit kafka message: %w", err)
	}

	c.recordKafkaMessage("processed")
	return nil
}

func (c *Consumer) recordKafkaMessage(result string) {
	if c.metrics == nil {
		return
	}
	c.metrics.IncKafkaMessage(result)
}

func (c *Consumer) recordEventProcessingError(reason string) {
	if c.metrics == nil {
		return
	}
	c.metrics.IncEventProcessingError(reason)
}
