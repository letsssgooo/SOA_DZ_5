package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const defaultInterval = time.Hour

type Config struct {
	HTTPAddr           string
	Interval           time.Duration
	ClickHouseAddr     string
	ClickHouseUser     string
	ClickHousePassword string
	PostgresHost       string
	PostgresPort       uint16
	PostgresUser       string
	PostgresPassword   string
	PostgresDB         string
}

func Load() (Config, error) {
	cfg := Config{HTTPAddr: ":8082", Interval: defaultInterval}
	if addr := os.Getenv("AGGREGATION_HTTP_ADDR"); addr != "" {
		cfg.HTTPAddr = addr
	}
	if value := os.Getenv("AGGREGATION_INTERVAL"); value != "" {
		interval, err := time.ParseDuration(value)
		if err != nil || interval <= 0 {
			return Config{}, fmt.Errorf("AGGREGATION_INTERVAL must be a positive duration, such as 1h")
		}
		cfg.Interval = interval
	}

	for _, field := range []struct {
		name  string
		value *string
	}{
		{"AGGREGATION_CLICKHOUSE_ADDR", &cfg.ClickHouseAddr},
		{"AGGREGATION_CLICKHOUSE_USER", &cfg.ClickHouseUser},
		{"AGGREGATION_CLICKHOUSE_PASSWORD", &cfg.ClickHousePassword},
		{"AGGREGATION_POSTGRES_HOST", &cfg.PostgresHost},
		{"AGGREGATION_POSTGRES_USER", &cfg.PostgresUser},
		{"AGGREGATION_POSTGRES_PASSWORD", &cfg.PostgresPassword},
		{"AGGREGATION_POSTGRES_DB", &cfg.PostgresDB},
	} {
		value := os.Getenv(field.name)
		if value == "" {
			return Config{}, fmt.Errorf("%s is required", field.name)
		}
		*field.value = value
	}

	const postgresPortEnv = "AGGREGATION_POSTGRES_PORT"
	port, err := strconv.ParseUint(os.Getenv(postgresPortEnv), 10, 16)
	if err != nil || port == 0 {
		return Config{}, fmt.Errorf("%s must be between 1 and 65535", postgresPortEnv)
	}
	cfg.PostgresPort = uint16(port)
	return cfg, nil
}
