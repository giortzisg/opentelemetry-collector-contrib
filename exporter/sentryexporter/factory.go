// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package sentryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sentryexporter"

import (
	"context"
	"fmt"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sentryexporter/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/sharedcomponent"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
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
	ctx context.Context,
	set exporter.Settings,
	config component.Config,
) (exporter.Traces, error) {
	sc, se, err := getOrCreateSentryExporter(config, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewTraces(
		ctx,
		set,
		config,
		se.pushTraceData,
		exporterhelper.WithStart(sc.Start),
		exporterhelper.WithShutdown(sc.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	config component.Config,
) (exporter.Logs, error) {
	sc, se, err := getOrCreateSentryExporter(config, set)
	if err != nil {
		return nil, err
	}
	return exporterhelper.NewLogs(
		ctx,
		set,
		config,
		se.pushLogData,
		exporterhelper.WithStart(sc.Start),
		exporterhelper.WithShutdown(sc.Shutdown),
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}

// getOrCreateSentryExporter creates a sentryExporter and caches it for a particular configuration.
func getOrCreateSentryExporter(cfg component.Config, set exporter.Settings) (*sharedcomponent.SharedComponent, *sentryExporter, error) {
	sc := exporters.GetOrAdd(cfg, func() component.Component {
		sentryConfig := cfg.(*Config)
		se, err := newSentryExporter(sentryConfig, set)
		if err != nil {
			return nil
		}
		return se
	})

	unwrapped := sc.Unwrap()
	if unwrapped == nil {
		return nil, nil, fmt.Errorf("failed to create sentry exporter")
	}
	return sc, unwrapped.(*sentryExporter), nil
}

var exporters = sharedcomponent.NewSharedComponents()
