package config

import (
	"errors"
	"testing"
	"time"
)

func TestLoadUsesDefaults(t *testing.T) {
	clearConfigEnv(t)

	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if config.HTTPAddr != defaultHTTPAddr {
		t.Fatalf("HTTPAddr = %q, want %q", config.HTTPAddr, defaultHTTPAddr)
	}
	if len(config.KafkaBrokers) != 1 || config.KafkaBrokers[0] != defaultKafkaBrokers {
		t.Fatalf("KafkaBrokers = %+v, want [%s]", config.KafkaBrokers, defaultKafkaBrokers)
	}
	if config.Window != defaultWindow {
		t.Fatalf("Window = %v, want %v", config.Window, defaultWindow)
	}
}

func TestLoadReadsEnvironment(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("KAFKA_BROKERS", "kafka-1:9092, kafka-2:9092")
	t.Setenv("WINDOW", "2m")
	t.Setenv("FUTURE_SKEW", "5s")
	t.Setenv("ABUSE_INTERVAL", "10s")
	t.Setenv("SNAPSHOT_INTERVAL", "500ms")
	t.Setenv("MAX_TOP_ITEMS", "25")

	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if config.HTTPAddr != ":9090" {
		t.Fatalf("HTTPAddr = %q, want :9090", config.HTTPAddr)
	}
	if len(config.KafkaBrokers) != 2 {
		t.Fatalf("KafkaBrokers length = %d, want 2", len(config.KafkaBrokers))
	}
	if config.Window != 2*time.Minute {
		t.Fatalf("Window = %v, want 2m", config.Window)
	}
	if config.FutureSkew != 5*time.Second {
		t.Fatalf("FutureSkew = %v, want 5s", config.FutureSkew)
	}
	if config.MaxTopItems != 25 {
		t.Fatalf("MaxTopItems = %d, want 25", config.MaxTopItems)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
		err   error
	}{
		{name: "invalid duration", key: "WINDOW", value: "bad", err: ErrInvalidDuration},
		{name: "empty brokers", key: "KAFKA_BROKERS", value: ",", err: ErrEmptyKafkaBrokers},
		{name: "invalid max top", key: "MAX_TOP_ITEMS", value: "0", err: ErrInvalidMaxTop},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv(tt.key, tt.value)

			_, err := Load()
			if !errors.Is(err, tt.err) {
				t.Fatalf("Load() error = %v, want %v", err, tt.err)
			}
		})
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		"HTTP_ADDR",
		"KAFKA_BROKERS",
		"KAFKA_TOPIC",
		"KAFKA_GROUP_ID",
		"WINDOW",
		"FUTURE_SKEW",
		"ABUSE_INTERVAL",
		"SNAPSHOT_INTERVAL",
		"MAX_TOP_ITEMS",
	} {
		t.Setenv(key, "")
	}
}
