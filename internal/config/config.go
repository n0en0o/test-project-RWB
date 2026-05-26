package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPAddr         = ":8080"
	defaultKafkaBrokers     = "localhost:9092"
	defaultKafkaTopic       = "search-events"
	defaultKafkaGroupID     = "trends-service"
	defaultWindow           = 5 * time.Minute
	defaultFutureSkew       = 10 * time.Second
	defaultAbuseInterval    = 30 * time.Second
	defaultSnapshotInterval = time.Second
	defaultMaxTopItems      = 100
)

var (
	ErrEmptyKafkaBrokers = errors.New("kafka brokers are required")
	ErrInvalidDuration   = errors.New("duration must be valid")
	ErrInvalidMaxTop     = errors.New("max top items must be positive")
)

type Config struct {
	HTTPAddr         string
	KafkaBrokers     []string
	KafkaTopic       string
	KafkaGroupID     string
	Window           time.Duration
	FutureSkew       time.Duration
	AbuseInterval    time.Duration
	AbuseTTL         time.Duration
	SnapshotInterval time.Duration
	MaxTopItems      int
}

func Load() (Config, error) {
	window, err := durationEnv("WINDOW", defaultWindow)
	if err != nil {
		return Config{}, err
	}
	futureSkew, err := durationEnv("FUTURE_SKEW", defaultFutureSkew)
	if err != nil {
		return Config{}, err
	}
	abuseInterval, err := durationEnv("ABUSE_INTERVAL", defaultAbuseInterval)
	if err != nil {
		return Config{}, err
	}
	snapshotInterval, err := durationEnv("SNAPSHOT_INTERVAL", defaultSnapshotInterval)
	if err != nil {
		return Config{}, err
	}
	maxTopItems, err := intEnv("MAX_TOP_ITEMS", defaultMaxTopItems)
	if err != nil {
		return Config{}, err
	}
	if maxTopItems <= 0 {
		return Config{}, ErrInvalidMaxTop
	}

	brokers := stringListEnv("KAFKA_BROKERS", defaultKafkaBrokers)
	if len(brokers) == 0 {
		return Config{}, ErrEmptyKafkaBrokers
	}

	return Config{
		HTTPAddr:         stringEnv("HTTP_ADDR", defaultHTTPAddr),
		KafkaBrokers:     brokers,
		KafkaTopic:       stringEnv("KAFKA_TOPIC", defaultKafkaTopic),
		KafkaGroupID:     stringEnv("KAFKA_GROUP_ID", defaultKafkaGroupID),
		Window:           window,
		FutureSkew:       futureSkew,
		AbuseInterval:    abuseInterval,
		AbuseTTL:         window,
		SnapshotInterval: snapshotInterval,
		MaxTopItems:      maxTopItems,
	}, nil
}

func stringEnv(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func stringListEnv(name, fallback string) []string {
	raw := stringEnv(name, fallback)
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func durationEnv(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(raw)
	if err != nil {
		return 0, ErrInvalidDuration
	}
	return duration, nil
}

func intEnv(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return value, nil
}
