// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sentryexporter

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/confmap/xconfmap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sentryexporter/internal/metadata"
)

func TestLoadConfig(t *testing.T) {
	t.Parallel()

	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	tests := []struct {
		id       component.ID
		expected component.Config
	}{
		{
			id: component.NewIDWithName(metadata.Type, ""),
			expected: &Config{
				DSNMode: &DSNModeConfig{
					DSN: "https://public_key@o123456.ingest.sentry.io/7654321",
				},
				ClientConfig: confighttp.ClientConfig{
					Timeout: 30 * time.Second,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "dsn_with_timeout"),
			expected: &Config{
				DSNMode: &DSNModeConfig{
					DSN:     "https://public_key@o123456.ingest.sentry.io/7654321",
					Timeout: 60 * time.Second,
				},
				ClientConfig: confighttp.ClientConfig{
					Timeout: 30 * time.Second,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "dsn_insecure"),
			expected: &Config{
				DSNMode: &DSNModeConfig{
					DSN:                "https://public_key@o123456.ingest.sentry.io/7654321",
					InsecureSkipVerify: true,
					Timeout:            30 * time.Second,
				},
				ClientConfig: confighttp.ClientConfig{
					Timeout: 30 * time.Second,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "dynamic_basic"),
			expected: &Config{
				DynamicMode: &DynamicModeConfig{
					URL:       "https://sentry.io",
					OrgSlug:   "my-org",
					AuthToken: configopaque.String("test-auth-token-12345"),
				},
				ClientConfig: confighttp.ClientConfig{
					Timeout: 30 * time.Second,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "dynamic_with_routing"),
			expected: &Config{
				DynamicMode: &DynamicModeConfig{
					URL:       "https://sentry.io",
					OrgSlug:   "my-org",
					AuthToken: configopaque.String("test-auth-token-12345"),
					Routing: RoutingConfig{
						AutoCreateProjects:  true,
						AttributeForProject: "service.name",
						ProjectMapping: map[string]string{
							"api-service": "backend-api",
							"web-service": "frontend-web",
						},
					},
				},
				ClientConfig: confighttp.ClientConfig{
					Timeout: 30 * time.Second,
				},
			},
		},
		{
			id: component.NewIDWithName(metadata.Type, "dynamic_full"),
			expected: &Config{
				DynamicMode: &DynamicModeConfig{
					URL:                "https://sentry.example.com",
					OrgSlug:            "example-org",
					AuthToken:          configopaque.String("full-test-token"),
					Timeout:            45 * time.Second,
					InsecureSkipVerify: true,
					Routing: RoutingConfig{
						AutoCreateProjects:  true,
						AttributeForProject: "deployment.environment.name",
						ProjectMapping: map[string]string{
							"production": "prod-project",
							"staging":    "stage-project",
						},
					},
				},
				ClientConfig: confighttp.ClientConfig{
					Timeout: 30 * time.Second,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			assert.NoError(t, xconfmap.Validate(cfg))
			assert.Equal(t, tt.expected, cfg)
		})
	}
}
