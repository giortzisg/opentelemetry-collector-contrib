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
	"go.uber.org/zap"
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
	sc, state, err := getOrCreateEndpointState(config, set)
	if err != nil {
		return nil, err
	}
	se := newSignalExporter(state, set.Logger.With(zap.String("signal", "traces")))
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
	sc, state, err := getOrCreateEndpointState(config, set)
	if err != nil {
		return nil, err
	}
	se := newSignalExporter(state, set.Logger.With(zap.String("signal", "logs")))
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

// getOrCreateEndpointState creates an endpointState and caches it for a particular configuration.
func getOrCreateEndpointState(cfg component.Config, set exporter.Settings) (*sharedcomponent.SharedComponent, *endpointState, error) {
	sc := states.GetOrAdd(cfg, func() component.Component {
		sentryConfig := cfg.(*Config)
		se, err := newEndpointState(sentryConfig, set)
		if err != nil {
			return nil
		}
		return se
	})

	unwrapped := sc.Unwrap()
	if unwrapped == nil {
		return nil, nil, fmt.Errorf("failed to create sentry exporter")
	}
	return sc, unwrapped.(*endpointState), nil
}

var states = sharedcomponent.NewSharedComponents()
