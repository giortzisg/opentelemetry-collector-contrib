// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sentryexporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestPareDSN(t *testing.T) {
	tests := []struct {
		name        string
		dsn         string
		expectError bool
		publicKey   string
		projectID   string
	}{
		{
			name:        "valid DSN",
			dsn:         "https://public_key@o123456.ingest.sentry.io/7654321",
			expectError: false,
			publicKey:   "public_key",
			projectID:   "7654321",
		},
		{
			name:        "DSN with path",
			dsn:         "https://key@sentry.example.com/path/12345",
			expectError: false,
			publicKey:   "key",
			projectID:   "12345",
		},
		{
			name:        "invalid DSN",
			dsn:         "://invalid",
			expectError: true,
		},
		{
			name:        "DSN without public key",
			dsn:         "https://sentry.io/12345",
			expectError: true,
		},
		{
			name:        "DSN without project ID",
			dsn:         "https://key@sentry.io",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endpoints, err := ParseDSN(tt.dsn)
			if tt.expectError {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, endpoints)
			assert.Equal(t, tt.publicKey, endpoints.PublicKey)
			assert.Contains(t, endpoints.TracesURL, tt.projectID)
			assert.Contains(t, endpoints.LogsURL, tt.projectID)
		})
	}
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

func TestPushTraceData(t *testing.T) {
	factory := NewFactory()
	requestReceived := false
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.Header.Get("x-sentry-auth"), "sentry sentry_key=")
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	cfg := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key@" + testServer.Listener.Addr().String() + "/7654321",
		},
	}

	set := exportertest.NewNopSettings(factory.Type())
	exp, err := newSentryExporter(cfg, set)
	require.NoError(t, err)
	exp.dsnEndpoint.TracesURL = testServer.URL

	traces := ptrace.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
	span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})

	err = exp.pushTraceData(context.Background(), traces)
	assert.NoError(t, err)
	assert.True(t, requestReceived, "Expected test server to receive request")
}

func TestPushLogData(t *testing.T) {
	factory := NewFactory()
	requestReceived := false
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestReceived = true
		assert.Equal(t, "POST", r.Method)
		assert.Contains(t, r.Header.Get("x-sentry-auth"), "sentry sentry_key=")
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	cfg := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key@" + testServer.Listener.Addr().String() + "/7654321",
		},
	}

	set := exportertest.NewNopSettings(factory.Type())
	exp, err := newSentryExporter(cfg, set)
	require.NoError(t, err)

	exp.dsnEndpoint.LogsURL = testServer.URL

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := sl.LogRecords().AppendEmpty()
	logRecord.Body().SetStr("test log message")

	err = exp.pushLogData(context.Background(), logs)
	assert.NoError(t, err)
	assert.True(t, requestReceived, "Expected test server to receive request")
}

func TestPushTraceDataEmpty(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key@o123456.ingest.sentry.io/7654321",
		},
	}

	set := exportertest.NewNopSettings(factory.Type())
	exp, err := newSentryExporter(cfg, set)
	require.NoError(t, err)

	traces := ptrace.NewTraces()
	err = exp.pushTraceData(context.Background(), traces)
	assert.NoError(t, err)
}

func TestPushLogDataEmpty(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key@o123456.ingest.sentry.io/7654321",
		},
	}

	set := exportertest.NewNopSettings(factory.Type())
	exp, err := newSentryExporter(cfg, set)
	require.NoError(t, err)

	logs := plog.NewLogs()
	err = exp.pushLogData(context.Background(), logs)
	assert.NoError(t, err)
}

func TestCacheInvalidationOn403(t *testing.T) {
	factory := NewFactory()
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"detail":"event submission rejected with_reason: ProjectId is invalid"}`))
	}))
	defer testServer.Close()

	cfg := &Config{
		DSNMode: &DSNModeConfig{
			DSN: "https://public_key@" + testServer.Listener.Addr().String() + "/7654321",
		},
	}

	set := exportertest.NewNopSettings(factory.Type())
	exp, err := newSentryExporter(cfg, set)
	require.NoError(t, err)

	projectSlug := "test-project"
	endpoint := &OTLPEndpoints{
		LogsURL:   testServer.URL,
		TracesURL: testServer.URL,
		PublicKey: "test-key",
	}
	exp.projectToEndpoint[projectSlug] = endpoint

	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", projectSlug)
	sl := rl.ScopeLogs().AppendEmpty()
	logRecord := sl.LogRecords().AppendEmpty()
	logRecord.Body().SetStr("test log")

	exp.config = &Config{
		DynamicMode: &DynamicModeConfig{
			OrgSlug: "test-org",
			Routing: RoutingConfig{
				AttributeForProject: "service.name",
			},
		},
	}
	exp.attributeKey = "service.name"

	err = exp.routeLogsByProject(context.Background(), logs)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "403")
	_, exists := exp.projectToEndpoint[projectSlug]
	assert.False(t, exists, "Cache entry should be invalidated after 403")
}
