// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sentryexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestNewFactory(t *testing.T) {
	factory := NewFactory()
	assert.NotNil(t, factory)
	assert.Equal(t, "sentry", factory.Type().String())
}

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig()
	assert.NotNil(t, cfg)
	err := cfg.(*Config).Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "either 'dsn_mode' or 'dynamic_mode' must be configured")
}

func TestCreateTracesExporter(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key@o123456.ingest.sentry.io/7654321",
		},
	}

	set := exportertest.NewNopSettings(factory.Type())
	exp, err := createTracesExporter(context.Background(), set, cfg)

	assert.NoError(t, err)
	assert.NotNil(t, exp)
}

func TestCreateLogsExporter(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key@o123456.ingest.sentry.io/7654321",
		},
	}

	set := exportertest.NewNopSettings(factory.Type())
	exp, err := createLogsExporter(context.Background(), set, cfg)

	assert.NoError(t, err)
	assert.NotNil(t, exp)
}

func TestCreateExporterWithInvalidConfig(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{} // Invalid: no DSNMode or DynamicMode config

	set := exportertest.NewNopSettings(factory.Type())
	_, err := createTracesExporter(context.Background(), set, cfg)

	assert.Error(t, err)
}

func TestSharedComponentSingleton(t *testing.T) {
	cfg := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key@o123456.ingest.sentry.io/7654321",
		},
	}

	factory := NewFactory()
	set := exportertest.NewNopSettings(factory.Type())

	tracesExp, err := factory.CreateTraces(context.Background(), set, cfg)
	require.NoError(t, err)
	require.NotNil(t, tracesExp)

	logsExp, err := factory.CreateLogs(context.Background(), set, cfg)
	require.NoError(t, err)
	require.NotNil(t, logsExp)

	sc1, se1, err := getOrCreateSentryExporter(cfg, set)
	require.NoError(t, err)
	sc2, se2, err := getOrCreateSentryExporter(cfg, set)
	require.NoError(t, err)

	assert.Same(t, sc1, sc2, "SharedComponents should be the same instance")
	assert.Same(t, se1, se2, "Unwrapped exporters should be the same instance")

	err = tracesExp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	err = logsExp.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)
	err = tracesExp.Shutdown(context.Background())
	require.NoError(t, err)
	err = logsExp.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestSharedComponentDifferentConfigs(t *testing.T) {
	cfg1 := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key1@o123456.ingest.sentry.io/7654321",
		},
	}

	cfg2 := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key2@o123456.ingest.sentry.io/7654321",
		},
	}

	factory := NewFactory()
	set := exportertest.NewNopSettings(factory.Type())

	exp1, err := factory.CreateTraces(context.Background(), set, cfg1)
	require.NoError(t, err)

	exp2, err := factory.CreateTraces(context.Background(), set, cfg2)
	require.NoError(t, err)

	_, se1, err := getOrCreateSentryExporter(cfg1, set)
	require.NoError(t, err)
	_, se2, err := getOrCreateSentryExporter(cfg2, set)
	require.NoError(t, err)

	assert.NotSame(t, se1, se2, "Different configs should create different exporter instances")

	err = exp1.Shutdown(context.Background())
	require.NoError(t, err)

	err = exp2.Shutdown(context.Background())
	require.NoError(t, err)
}
