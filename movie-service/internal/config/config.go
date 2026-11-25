package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const generatorEnabledEnv = "MOVIE_GENERATOR_ENABLED"

type Config struct {
	HTTPAddr          string
	KafkaBrokers      []string
	SchemaRegistryURL string
	GeneratorEnabled  bool
}

func Load() (Config, error) {
	generatorEnabled, err := strconv.ParseBool(envOrDefault(generatorEnabledEnv, "false"))
	if err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", generatorEnabledEnv, err)
	}

	brokers := strings.Split(envOrDefault("MOVIE_KAFKA_BROKERS", "localhost:9092"), ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return Config{
		HTTPAddr:          envOrDefault("MOVIE_HTTP_ADDR", ":8080"),
		KafkaBrokers:      brokers,
		SchemaRegistryURL: envOrDefault("MOVIE_SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		GeneratorEnabled:  generatorEnabled,
	}, nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
