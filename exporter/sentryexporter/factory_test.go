// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sentryexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
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
