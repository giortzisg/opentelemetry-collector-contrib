// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package sentryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sentryexporter"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/exporter"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sentryexporter/internal/metadata"
)

// NewFactory creates a factory for Sentry exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig: confighttp.ClientConfig{
			Timeout: 30 * time.Second,
		},
	}
}

func createTracesExporter(
	_ context.Context,
	params exporter.Settings,
	config component.Config,
) (exporter.Traces, error) {
	sentryConfig, ok := config.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type: %T", config)
	}

	// Create exporter based on sentry config.
	return newSentryExporter(sentryConfig, params)
}

func createLogsExporter(
	_ context.Context,
	set exporter.Settings,
	config component.Config,
) (exporter.Logs, error) {
	sentryConfig, ok := config.(*Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type: %T", config)
	}

	// Create exporter based on sentry config.
	return newSentryExporter(sentryConfig, set)
}
