// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sentryexporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/testdata"
)

func TestParseDSN(t *testing.T) {
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

func TestExporterDataFlow(t *testing.T) {
	type testCase struct {
		name string

		config *Config

		serverHandler    func(*testing.T, *int, *int) http.HandlerFunc
		setupMocks       func(*mockSentryClient)
		prePopulateCache func(*endpointState, string)
		setupOnStart     func(*endpointState) error

		resourceAttributes map[string]string

		expectedTraceRequests int
		expectedLogRequests   int
		expectedError         bool

		assertExpectations func(*testing.T, *endpointState, *mockSentryClient)
	}

	tests := []testCase{
		{
			name: "dsn_mode_success",
			config: &Config{
				DSNMode: &DSNModeConfig{
					DSN: "http://public_key@localhost/7654321",
				},
			},
			serverHandler: func(t *testing.T, traceReqs, logReqs *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/traces" {
						*traceReqs++
					} else if r.URL.Path == "/logs" {
						*logReqs++
					}
					w.WriteHeader(http.StatusOK)
				}
			},
			setupMocks: nil,
			prePopulateCache: func(state *endpointState, testServerAddr string) {
				state.dsnEndpoint.TracesURL = "http://" + testServerAddr + "/traces"
				state.dsnEndpoint.LogsURL = "http://" + testServerAddr + "/logs"
			},
			setupOnStart:          nil,
			resourceAttributes:    nil,
			expectedTraceRequests: 1,
			expectedLogRequests:   1,
			expectedError:         false,
		},
		{
			name: "dynamic_mode_cached_project",
			config: &Config{
				DynamicMode: &DynamicModeConfig{
					OrgSlug:   "test-org",
					AuthToken: "test-token",
					URL:       "https://sentry.io",
					Routing: RoutingConfig{
						AttributeForProject: "service.name",
					},
				},
			},
			serverHandler: func(t *testing.T, traceReqs, logReqs *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/traces" {
						*traceReqs++
					} else if r.URL.Path == "/logs" {
						*logReqs++
					}
					w.WriteHeader(http.StatusOK)
				}
			},
			setupMocks: func(mc *mockSentryClient) {
				mc.On("GetAllProjects", mock.Anything, "test-org").
					Return([]ProjectInfo{}, nil)
			},
			prePopulateCache: func(state *endpointState, testServerAddr string) {
				state.projectToEndpoint["my-service"] = &OTLPEndpoints{
					TracesURL: "http://" + testServerAddr + "/traces",
					LogsURL:   "http://" + testServerAddr + "/logs",
					PublicKey: "test-key",
				}
			},
			resourceAttributes: map[string]string{
				"service.name": "my-service",
			},
			expectedTraceRequests: 1,
			expectedLogRequests:   1,
			expectedError:         false,
			assertExpectations: func(t *testing.T, state *endpointState, mc *mockSentryClient) {
				mc.AssertNotCalled(t, "CreateProject")
			},
		},
		{
			name: "dynamic_mode_project_creation",
			config: &Config{
				DynamicMode: &DynamicModeConfig{
					OrgSlug:   "test-org",
					AuthToken: "test-token",
					URL:       "https://sentry.io",
					Routing: RoutingConfig{
						AttributeForProject: "service.name",
						AutoCreateProjects:  true,
					},
				},
			},
			serverHandler: func(t *testing.T, traceReqs, logReqs *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/traces" {
						*traceReqs++
					} else if r.URL.Path == "/logs" {
						*logReqs++
					}
					w.WriteHeader(http.StatusOK)
				}
			},
			setupMocks: func(mc *mockSentryClient) {
				mc.On("GetAllProjects", mock.Anything, "test-org").
					Return([]ProjectInfo{}, nil)
			},
			prePopulateCache: nil,
			setupOnStart: func(state *endpointState) error {
				state.defaultTeamSlug = "test-team"
				return nil
			},
			resourceAttributes: map[string]string{
				"service.name":           "new-service",
				"telemetry.sdk.language": "python",
			},
			expectedTraceRequests: 1,
			expectedLogRequests:   1,
			expectedError:         false,
			assertExpectations: func(t *testing.T, state *endpointState, mc *mockSentryClient) {
				mc.AssertCalled(t, "GetOTLPEndpoints", mock.Anything, "test-org", "new-service")
				mc.AssertCalled(t, "CreateProject", mock.Anything, "test-org", "test-team", "new-service", "new-service", "python")
				assert.Len(t, state.projectToEndpoint, 1, "Should have cached the new project")
			},
		},
		{
			name: "dynamic_mode_missing_routing_attribute",
			config: &Config{
				DynamicMode: &DynamicModeConfig{
					OrgSlug:   "test-org",
					AuthToken: "test-token",
					URL:       "https://sentry.io",
					Routing: RoutingConfig{
						AttributeForProject: "service.name",
					},
				},
			},
			serverHandler: func(t *testing.T, traceReqs, logReqs *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					t.Fatalf("Should not make HTTP request when routing attribute is missing")
				}
			},
			setupMocks: func(mc *mockSentryClient) {
				mc.On("GetAllProjects", mock.Anything, "test-org").
					Return([]ProjectInfo{}, nil)
			},
			prePopulateCache:      nil,
			resourceAttributes:    nil,
			expectedTraceRequests: 0,
			expectedLogRequests:   0,
			expectedError:         false,
			assertExpectations: func(t *testing.T, state *endpointState, mc *mockSentryClient) {
				mc.AssertNotCalled(t, "GetOTLPEndpoints")
				mc.AssertNotCalled(t, "CreateProject")
			},
		},
		{
			name: "dynamic_mode_cache_invalidation_403",
			config: &Config{
				DynamicMode: &DynamicModeConfig{
					OrgSlug:   "test-org",
					AuthToken: "test-token",
					URL:       "https://sentry.io",
					Routing: RoutingConfig{
						AttributeForProject: "service.name",
						AutoCreateProjects:  false,
					},
				},
			},
			serverHandler: func(t *testing.T, traceReqs, logReqs *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusForbidden)
					_, _ = w.Write([]byte(`{"detail":"event submission rejected with_reason: ProjectId is invalid"}`))
				}
			},
			setupMocks: func(mc *mockSentryClient) {
				mc.On("GetAllProjects", mock.Anything, "test-org").
					Return([]ProjectInfo{}, nil)
				mc.On("GetOTLPEndpoints", mock.Anything, "test-org", "test-service").
					Return((*OTLPEndpoints)(nil), assert.AnError)
			},
			prePopulateCache: func(state *endpointState, testServerAddr string) {
				state.projectToEndpoint["test-service"] = &OTLPEndpoints{
					TracesURL: "http://" + testServerAddr + "/traces",
					LogsURL:   "http://" + testServerAddr + "/logs",
					PublicKey: "test-key",
				}
			},
			resourceAttributes: map[string]string{
				"service.name": "test-service",
			},
			expectedTraceRequests: 0,
			expectedLogRequests:   0,
			expectedError:         true,
			assertExpectations: func(t *testing.T, state *endpointState, mc *mockSentryClient) {
				_, exists := state.projectToEndpoint["test-service"]
				assert.False(t, exists, "Cache should be invalidated after 403")
			},
		},
		{
			name: "dynamic_mode_500_error_keeps_cache",
			config: &Config{
				DynamicMode: &DynamicModeConfig{
					OrgSlug:   "test-org",
					AuthToken: "test-token",
					URL:       "https://sentry.io",
					Routing: RoutingConfig{
						AttributeForProject: "service.name",
					},
				},
			},
			serverHandler: func(t *testing.T, traceReqs, logReqs *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte("Internal Server Error"))
				}
			},
			setupMocks: func(mc *mockSentryClient) {
				mc.On("GetAllProjects", mock.Anything, "test-org").
					Return([]ProjectInfo{}, nil)
			},
			prePopulateCache: func(state *endpointState, testServerAddr string) {
				state.projectToEndpoint["test-service"] = &OTLPEndpoints{
					TracesURL: "http://" + testServerAddr + "/traces",
					LogsURL:   "http://" + testServerAddr + "/logs",
					PublicKey: "test-key",
				}
			},
			resourceAttributes: map[string]string{
				"service.name": "test-service",
			},
			expectedTraceRequests: 0,
			expectedLogRequests:   0,
			expectedError:         true,
			assertExpectations: func(t *testing.T, state *endpointState, mc *mockSentryClient) {
				_, exists := state.projectToEndpoint["test-service"]
				assert.True(t, exists, "Cache should NOT be invalidated on 500 errors")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var traceRequests, logRequests int

			handler := tt.serverHandler(t, &traceRequests, &logRequests)
			testServer := httptest.NewServer(handler)
			defer testServer.Close()

			set := exportertest.NewNopSettings(NewFactory().Type())
			state, err := newEndpointState(tt.config, set)
			require.NoError(t, err)

			var mockClient *mockSentryClient
			if tt.config.IsDynamicMode() {
				mockClient = &mockSentryClient{}
				state.sentryClient = mockClient
				if tt.setupMocks != nil {
					tt.setupMocks(mockClient)
				}
			}

			if tt.prePopulateCache != nil {
				tt.prePopulateCache(state, testServer.Listener.Addr().String())
			}

			if tt.setupOnStart != nil {
				err = tt.setupOnStart(state)
				require.NoError(t, err)
			}

			err = state.Start(context.Background(), componenttest.NewNopHost())
			require.NoError(t, err)
			defer func() {
				err = state.Shutdown(context.Background())
				require.NoError(t, err)
			}()

			exp := newSignalExporter(state, set.Logger.With(zap.String("test", tt.name)))

			if tt.config.IsDynamicMode() && tt.prePopulateCache == nil && tt.resourceAttributes != nil {
				endpoint := &OTLPEndpoints{
					TracesURL: "http://" + testServer.Listener.Addr().String() + "/traces",
					LogsURL:   "http://" + testServer.Listener.Addr().String() + "/logs",
					PublicKey: "new-key",
				}

				mockClient.On("GetOTLPEndpoints", mock.Anything, "test-org", mock.Anything).
					Return((*OTLPEndpoints)(nil), assert.AnError).Once()

				mockClient.On("CreateProject", mock.Anything, "test-org", "test-team", mock.Anything, mock.Anything, mock.Anything).
					Return(&ProjectInfo{}, nil).Once()

				mockClient.On("GetOTLPEndpoints", mock.Anything, "test-org", mock.Anything).
					Return(endpoint, nil).Once()
			}

			traces := testdata.GenerateTracesTwoSpansSameResource()
			for k, v := range tt.resourceAttributes {
				traces.ResourceSpans().At(0).Resource().Attributes().PutStr(k, v)
			}
			err = exp.pushTraceData(context.Background(), traces)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			logs := testdata.GenerateLogsTwoLogRecordsSameResource()
			for k, v := range tt.resourceAttributes {
				logs.ResourceLogs().At(0).Resource().Attributes().PutStr(k, v)
			}
			err = exp.pushLogData(context.Background(), logs)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectedTraceRequests, traceRequests)
			assert.Equal(t, tt.expectedLogRequests, logRequests)

			if tt.assertExpectations != nil {
				tt.assertExpectations(t, state, mockClient)
			}

			if mockClient != nil {
				mockClient.AssertExpectations(t)
			}
		})
	}
}

func TestStartPrePopulatesCache(t *testing.T) {
	t.Run("loads_existing_projects", func(t *testing.T) {
		cfg := &Config{
			DynamicMode: &DynamicModeConfig{
				OrgSlug:   "test-org",
				AuthToken: "test-token",
				URL:       "https://sentry.io",
			},
		}

		set := exportertest.NewNopSettings(NewFactory().Type())
		state, err := newEndpointState(cfg, set)
		require.NoError(t, err)

		mockClient := &mockSentryClient{}
		state.sentryClient = mockClient

		projects := []ProjectInfo{
			{Slug: "project1", Teams: []TeamInfo{{Slug: "team1"}}},
			{Slug: "project2", Teams: []TeamInfo{{Slug: "team1"}}},
		}

		endpoint1 := &OTLPEndpoints{
			TracesURL: "https://example.com/project1/traces",
			LogsURL:   "https://example.com/project1/logs",
			PublicKey: "key1",
		}

		endpoint2 := &OTLPEndpoints{
			TracesURL: "https://example.com/project2/traces",
			LogsURL:   "https://example.com/project2/logs",
			PublicKey: "key2",
		}

		mockClient.On("GetAllProjects", mock.Anything, "test-org").
			Return(projects, nil)
		mockClient.On("GetOTLPEndpoints", mock.Anything, "test-org", "project1").
			Return(endpoint1, nil)
		mockClient.On("GetOTLPEndpoints", mock.Anything, "test-org", "project2").
			Return(endpoint2, nil)

		err = state.Start(context.Background(), componenttest.NewNopHost())
		require.NoError(t, err)

		assert.Len(t, state.projectToEndpoint, 2)
		assert.Equal(t, endpoint1, state.projectToEndpoint["project1"])
		assert.Equal(t, endpoint2, state.projectToEndpoint["project2"])
		assert.Equal(t, "team1", state.defaultTeamSlug)

		mockClient.AssertExpectations(t)
	})
}

func TestGetOrCreateProjectEndpoint(t *testing.T) {
	t.Run("project_exists_in_cache", func(t *testing.T) {
		cfg := &Config{
			DynamicMode: &DynamicModeConfig{
				OrgSlug:   "test-org",
				AuthToken: "test-token",
				URL:       "https://sentry.io",
			},
		}

		set := exportertest.NewNopSettings(NewFactory().Type())
		state, err := newEndpointState(cfg, set)
		require.NoError(t, err)

		mockClient := &mockSentryClient{}
		state.sentryClient = mockClient

		projects := []ProjectInfo{
			{
				Slug: "project1",
				Teams: []TeamInfo{
					{Slug: "team1"},
				},
			},
			{
				Slug: "project2",
				Teams: []TeamInfo{
					{Slug: "team1"},
				},
			},
		}

		endpoint1 := &OTLPEndpoints{
			TracesURL: "https://example.com/project1/traces",
			LogsURL:   "https://example.com/project1/logs",
			PublicKey: "key1",
		}

		endpoint2 := &OTLPEndpoints{
			TracesURL: "https://example.com/project2/traces",
			LogsURL:   "https://example.com/project2/logs",
			PublicKey: "key2",
		}

		mockClient.On("GetAllProjects", mock.Anything, "test-org").
			Return(projects, nil)

		mockClient.On("GetOTLPEndpoints", mock.Anything, "test-org", "project1").
			Return(endpoint1, nil)

		mockClient.On("GetOTLPEndpoints", mock.Anything, "test-org", "project2").
			Return(endpoint2, nil)

		err = state.Start(context.Background(), componenttest.NewNopHost())
		require.NoError(t, err)

		assert.Len(t, state.projectToEndpoint, 2, "Should have cached 2 projects")
		assert.Equal(t, endpoint1, state.projectToEndpoint["project1"])
		assert.Equal(t, endpoint2, state.projectToEndpoint["project2"])
		assert.Equal(t, "team1", state.defaultTeamSlug, "Should set default team")

		mockClient.AssertExpectations(t)
	})

	t.Run("handles_get_all_projects_error", func(t *testing.T) {
		cfg := &Config{
			DynamicMode: &DynamicModeConfig{
				OrgSlug:   "test-org",
				AuthToken: "test-token",
				URL:       "https://sentry.io",
			},
		}

		set := exportertest.NewNopSettings(NewFactory().Type())
		state, err := newEndpointState(cfg, set)
		require.NoError(t, err)

		mockClient := &mockSentryClient{}
		state.sentryClient = mockClient

		mockClient.On("GetAllProjects", mock.Anything, "test-org").
			Return(([]ProjectInfo)(nil), assert.AnError)

		err = state.Start(context.Background(), componenttest.NewNopHost())
		require.NoError(t, err, "Should not fail on pre-population error")

		assert.Empty(t, state.projectToEndpoint, "Cache should be empty")

		mockClient.AssertExpectations(t)
	})

	t.Run("continues_on_endpoint_fetch_error", func(t *testing.T) {
		cfg := &Config{
			DynamicMode: &DynamicModeConfig{
				OrgSlug:   "test-org",
				AuthToken: "test-token",
				URL:       "https://sentry.io",
			},
		}

		set := exportertest.NewNopSettings(NewFactory().Type())
		state, err := newEndpointState(cfg, set)
		require.NoError(t, err)

		mockClient := &mockSentryClient{}
		state.sentryClient = mockClient

		projects := []ProjectInfo{
			{Slug: "project1", Teams: []TeamInfo{{Slug: "team1"}}},
			{Slug: "project2", Teams: []TeamInfo{{Slug: "team1"}}},
		}

		endpoint2 := &OTLPEndpoints{
			TracesURL: "https://example.com/project2/traces",
			LogsURL:   "https://example.com/project2/logs",
			PublicKey: "key2",
		}

		mockClient.On("GetAllProjects", mock.Anything, "test-org").
			Return(projects, nil)

		mockClient.On("GetOTLPEndpoints", mock.Anything, "test-org", "project1").
			Return((*OTLPEndpoints)(nil), assert.AnError)

		mockClient.On("GetOTLPEndpoints", mock.Anything, "test-org", "project2").
			Return(endpoint2, nil)

		err = state.Start(context.Background(), componenttest.NewNopHost())
		require.NoError(t, err)

		assert.Len(t, state.projectToEndpoint, 1, "Should have cached 1 project (skipped the error)")
		assert.Equal(t, endpoint2, state.projectToEndpoint["project2"])

		mockClient.AssertExpectations(t)
	})
}
