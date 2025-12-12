// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sentryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sentryexporter"

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

var errExporterShuttingDown = errors.New("sentry exporter is shutting down")

// endpointState holds shared mutable state (client, caches) across signal exporters.
type endpointState struct {
	config *Config

	baseLogger *zap.Logger
	client     *http.Client

	dsnEndpoint *OTLPEndpoints

	sentryClient SentryAPIClient

	projectToEndpoint map[string]*OTLPEndpoints
	projectMapMu      sync.RWMutex
	projectCreationMu sync.Mutex

	attributeKey    string
	projectMapping  map[string]string
	defaultTeamSlug string

	inflight singleflight.Group

	startOnce    sync.Once
	startErr     error
	shutdownOnce sync.Once
	closing      atomic.Bool
}

// sentryExporter is a per-signal wrapper that forwards to the shared endpointState.
type sentryExporter struct {
	logger *zap.Logger
	state  *endpointState
}

func newEndpointState(config *Config, set exporter.Settings) (*endpointState, error) {
	if config.Environment != "" {
		set.Logger.Warn(
			"The 'environment' field is deprecated and ignored. " +
				"Use OpenTelemetry resource attributes instead. " +
				"Add 'deployment.environment' attribute via a resource processor.",
		)
	}

	client := &http.Client{
		Timeout: config.GetTimeout(),
	}

	if config.GetInsecureSkipVerify() {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		}
		transport.TLSClientConfig.InsecureSkipVerify = true
		client.Transport = transport
	}

	state := &endpointState{
		config:            config,
		baseLogger:        set.Logger,
		client:            client,
		projectToEndpoint: make(map[string]*OTLPEndpoints),
	}

	switch {
	case config.IsDSNMode():
		endpoint, err := ParseDSN(config.DSNMode.DSN)
		if err != nil {
			return nil, fmt.Errorf("failed to parse DSN: %w", err)
		}
		state.dsnEndpoint = endpoint
		set.Logger.Info("Configured in DSN mode", zap.String("dsn", config.DSNMode.DSN))

	case config.IsDynamicMode():
		state.sentryClient = NewSentryClient(
			config.DynamicMode.URL,
			string(config.DynamicMode.AuthToken),
			client,
		)

		state.attributeKey = config.DynamicMode.Routing.AttributeForProject
		if state.attributeKey == "" {
			state.attributeKey = DefaultAttributeForProject
		}

		state.projectMapping = config.DynamicMode.Routing.ProjectMapping
		set.Logger.Info("Configured in dynamic mode",
			zap.String("org", config.DynamicMode.OrgSlug),
			zap.String("routing_attribute", state.attributeKey),
			zap.Bool("auto_create_projects", config.DynamicMode.Routing.AutoCreateProjects))

	default:
		return nil, errors.New("exporter must be configured in either DSN or dynamic mode")
	}

	return state, nil
}

func newSignalExporter(state *endpointState, logger *zap.Logger) *sentryExporter {
	return &sentryExporter{
		logger: logger,
		state:  state,
	}
}

// pushTraceData takes an incoming OpenTelemetry trace, and forwards it to the Sentry OTLP endpoint.
func (e *sentryExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	return e.state.pushTraceData(ctx, e.logger, td)
}

func (s *endpointState) pushTraceData(ctx context.Context, logger *zap.Logger, td ptrace.Traces) error {
	if s.closing.Load() {
		return errExporterShuttingDown
	}
	if td.SpanCount() == 0 {
		return nil
	}

	if s.config.IsDSNMode() {
		return s.sendTracesToEndpoint(ctx, logger, td, s.dsnEndpoint)
	}

	return s.routeTracesByProject(ctx, logger, td)
}

// routeTracesByProject splits traces by project and sends each batch to the appropriate endpoint
func (s *endpointState) routeTracesByProject(ctx context.Context, logger *zap.Logger, td ptrace.Traces) error {
	type projectKey struct {
		slug     string
		platform string
	}
	projectGroups := make(map[projectKey]ptrace.Traces)

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		attrs := rs.Resource().Attributes()
		projectSlug := s.extractProjectSlug(attrs)

		if projectSlug == "" {
			logger.Warn("Dropping trace: missing required routing attribute",
				zap.String("attribute", s.attributeKey))
			continue
		}

		platform := s.extractPlatform(attrs)
		key := projectKey{slug: projectSlug, platform: platform}

		if _, exists := projectGroups[key]; !exists {
			projectGroups[key] = ptrace.NewTraces()
		}
		rs.CopyTo(projectGroups[key].ResourceSpans().AppendEmpty())
	}

	var errs error
	for key, traces := range projectGroups {
		endpoint, err := s.getOrCreateProjectEndpoint(ctx, logger, key.slug, key.platform)
		if err != nil {
			logger.Error("Failed to get endpoint for project",
				zap.String("project", key.slug),
				zap.Error(err))
			errs = errors.Join(errs, err)
			continue
		}

		if err := s.sendTracesToEndpoint(ctx, logger, traces, endpoint); err != nil {
			var httpErr *sentryHTTPError
			if errors.As(err, &httpErr) {
				if httpErr.statusCode == http.StatusForbidden && strings.Contains(httpErr.body, "event submission rejected with_reason: ProjectId") {
					logger.Warn("Project may have been deleted, removing from cache and retrying",
						zap.String("project", key.slug),
						zap.Int("status_code", httpErr.statusCode))
					s.projectMapMu.Lock()
					delete(s.projectToEndpoint, key.slug)
					s.projectMapMu.Unlock()

					newEndpoint, retryErr := s.getOrCreateProjectEndpoint(ctx, logger, key.slug, key.platform)
					if retryErr != nil {
						logger.Error("Failed to get endpoint for project on retry",
							zap.String("project", key.slug),
							zap.Error(retryErr))
						errs = errors.Join(errs, retryErr)
						continue
					}

					if retryErr := s.sendTracesToEndpoint(ctx, logger, traces, newEndpoint); retryErr != nil {
						logger.Error("Failed to send traces to project on retry",
							zap.String("project", key.slug),
							zap.Error(retryErr))
						errs = errors.Join(errs, retryErr)
						continue
					}
					continue
				}
			}

			logger.Error("Failed to send traces to project",
				zap.String("project", key.slug),
				zap.Error(err))
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

// sendTracesToEndpoint sends traces to a specific endpoint
func (s *endpointState) sendTracesToEndpoint(ctx context.Context, logger *zap.Logger, td ptrace.Traces, endpoint *OTLPEndpoints) error {
	request := ptraceotlp.NewExportRequestFromTraces(td)
	data, err := request.MarshalProto()
	if err != nil {
		return fmt.Errorf("failed to marshal traces: %w", err)
	}

	authHeader := fmt.Sprintf("sentry sentry_key=%s", endpoint.PublicKey)
	if err := s.sendOTLPData(ctx, logger, endpoint.TracesURL, data, authHeader); err != nil {
		return fmt.Errorf("failed to send traces to Sentry: %w", err)
	}

	logger.Debug("Successfully sent traces to Sentry",
		zap.Int("span_count", td.SpanCount()))

	return nil
}

// Start starts the exporter
func (s *endpointState) Start(ctx context.Context, _ component.Host) error {
	s.startOnce.Do(func() {
		if s.config.IsDSNMode() {
			s.baseLogger.Info("Starting sentryexporter in DSN mode",
				zap.String("traces_endpoint", s.dsnEndpoint.TracesURL),
				zap.String("logs_endpoint", s.dsnEndpoint.LogsURL))
			return
		}

		s.baseLogger.Info("Starting sentryexporter in dynamic mode",
			zap.String("org", s.config.DynamicMode.OrgSlug))

		projects, err := s.sentryClient.GetAllProjects(ctx, s.config.DynamicMode.OrgSlug)
		if err != nil {
			s.baseLogger.Warn("Failed to pre-populate project cache",
				zap.Error(err))
			s.startErr = nil
			return
		}

		s.projectMapMu.Lock()
		for _, project := range projects {
			if s.defaultTeamSlug == "" && len(project.Teams) > 0 {
				s.defaultTeamSlug = project.Teams[0].Slug
			}

			endpoint, err := s.sentryClient.GetOTLPEndpoints(ctx, s.config.DynamicMode.OrgSlug, project.Slug)
			if err != nil {
				s.baseLogger.Warn("Failed to fetch endpoint for project",
					zap.String("project", project.Slug),
					zap.Error(err))
				continue
			}
			s.projectToEndpoint[project.Slug] = endpoint
		}
		s.projectMapMu.Unlock()
	})

	return s.startErr
}

// Shutdown stops the exporter
func (s *endpointState) Shutdown(_ context.Context) error {
	var shutdownErr error
	s.shutdownOnce.Do(func() {
		s.closing.Store(true)
		s.baseLogger.Info("Shutting down sentryexporter")
		if s.client != nil {
			s.client.CloseIdleConnections()
		}
	})
	return shutdownErr
}

func (e *sentryExporter) pushLogData(ctx context.Context, ld plog.Logs) error {
	return e.state.pushLogData(ctx, e.logger, ld)
}

func (s *endpointState) pushLogData(ctx context.Context, logger *zap.Logger, ld plog.Logs) error {
	if s.closing.Load() {
		return errExporterShuttingDown
	}
	if ld.LogRecordCount() == 0 {
		return nil
	}

	if s.config.IsDSNMode() {
		return s.sendLogsToEndpoint(ctx, logger, ld, s.dsnEndpoint)
	}

	return s.routeLogsByProject(ctx, logger, ld)
}

func (s *endpointState) routeLogsByProject(ctx context.Context, logger *zap.Logger, ld plog.Logs) error {
	type projectKey struct {
		slug     string
		platform string
	}
	projectGroups := make(map[projectKey]plog.Logs)

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		attrs := rl.Resource().Attributes()
		projectSlug := s.extractProjectSlug(attrs)

		if projectSlug == "" {
			logger.Warn("Dropping logs: missing required routing attribute",
				zap.String("attribute", s.attributeKey))
			continue
		}

		platform := s.extractPlatform(attrs)
		key := projectKey{slug: projectSlug, platform: platform}

		if _, exists := projectGroups[key]; !exists {
			projectGroups[key] = plog.NewLogs()
		}
		rl.CopyTo(projectGroups[key].ResourceLogs().AppendEmpty())
	}

	var errs error
	for key, logs := range projectGroups {
		endpoint, err := s.getOrCreateProjectEndpoint(ctx, logger, key.slug, key.platform)
		if err != nil {
			logger.Error("Failed to get endpoint for project",
				zap.String("project", key.slug),
				zap.Error(err))
			errs = errors.Join(errs, err)
			continue
		}

		if err := s.sendLogsToEndpoint(ctx, logger, logs, endpoint); err != nil {
			var httpErr *sentryHTTPError
			if errors.As(err, &httpErr) {
				if httpErr.statusCode == http.StatusForbidden && strings.Contains(httpErr.body, "event submission rejected with_reason: ProjectId") {
					logger.Warn("Project may have been deleted, removing from cache and retrying",
						zap.String("project", key.slug),
						zap.Int("status_code", httpErr.statusCode))
					s.projectMapMu.Lock()
					delete(s.projectToEndpoint, key.slug)
					s.projectMapMu.Unlock()

					newEndpoint, retryErr := s.getOrCreateProjectEndpoint(ctx, logger, key.slug, key.platform)
					if retryErr != nil {
						logger.Error("Failed to get endpoint for project on retry",
							zap.String("project", key.slug),
							zap.Error(retryErr))
						errs = errors.Join(errs, retryErr)
						continue
					}

					if retryErr := s.sendLogsToEndpoint(ctx, logger, logs, newEndpoint); retryErr != nil {
						logger.Error("Failed to send logs to project on retry",
							zap.String("project", key.slug),
							zap.Error(retryErr))
						errs = errors.Join(errs, retryErr)
						continue
					}
					continue
				}
			}

			logger.Error("Failed to send logs to project",
				zap.String("project", key.slug),
				zap.Error(err))
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

func (s *endpointState) sendLogsToEndpoint(ctx context.Context, logger *zap.Logger, ld plog.Logs, endpoint *OTLPEndpoints) error {
	request := plogotlp.NewExportRequestFromLogs(ld)
	data, err := request.MarshalProto()
	if err != nil {
		return fmt.Errorf("failed to marshal logs: %w", err)
	}

	authHeader := fmt.Sprintf("sentry sentry_key=%s", endpoint.PublicKey)
	if err := s.sendOTLPData(ctx, logger, endpoint.LogsURL, data, authHeader); err != nil {
		return fmt.Errorf("failed to send logs to Sentry: %w", err)
	}

	logger.Debug("Successfully sent logs to Sentry",
		zap.Int("log_count", ld.LogRecordCount()))

	return nil
}

// sentryHTTPError represents an HTTP error from Sentry
type sentryHTTPError struct {
	statusCode int
	body       string
}

func (e *sentryHTTPError) Error() string {
	return fmt.Sprintf("request failed with status %d: %s", e.statusCode, e.body)
}

func (s *endpointState) sendOTLPData(ctx context.Context, logger *zap.Logger, endpoint string, data []byte, authHeader string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("x-sentry-auth", authHeader)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &sentryHTTPError{
			statusCode: resp.StatusCode,
			body:       string(body),
		}
	}

	logger.Debug("Successfully sent data to Sentry",
		zap.Int("status_code", resp.StatusCode),
		zap.Int("data_size", len(data)))

	return nil
}

func (s *endpointState) extractProjectSlug(attrs pcommon.Map) string {
	attrValue, exists := attrs.Get(s.attributeKey)
	if !exists {
		return ""
	}

	serviceName := attrValue.Str()
	if serviceName == "" {
		return ""
	}

	if s.projectMapping != nil {
		if mappedSlug, ok := s.projectMapping[serviceName]; ok {
			return mappedSlug
		}
	}

	return serviceName
}

func (*endpointState) extractPlatform(attrs pcommon.Map) string {
	if lang, exists := attrs.Get("telemetry.sdk.language"); exists {
		return lang.Str()
	}
	return "other"
}

func (s *endpointState) getOrCreateProjectEndpoint(ctx context.Context, logger *zap.Logger, projectSlug, platform string) (*OTLPEndpoints, error) {
	s.projectMapMu.RLock()
	if cached, ok := s.projectToEndpoint[projectSlug]; ok {
		s.projectMapMu.RUnlock()
		return cached, nil
	}
	s.projectMapMu.RUnlock()

	result, err, _ := s.inflight.Do(projectSlug, func() (any, error) {
		s.projectMapMu.RLock()
		if cached, ok := s.projectToEndpoint[projectSlug]; ok {
			s.projectMapMu.RUnlock()
			return cached, nil
		}
		s.projectMapMu.RUnlock()

		endpoint, fetchErr := s.sentryClient.GetOTLPEndpoints(ctx, s.config.DynamicMode.OrgSlug, projectSlug)
		if fetchErr == nil {
			s.projectMapMu.Lock()
			s.projectToEndpoint[projectSlug] = endpoint
			s.projectMapMu.Unlock()
			return endpoint, nil
		}

		if !s.config.DynamicMode.Routing.AutoCreateProjects {
			return nil, fmt.Errorf("project %s not found and auto_create_projects is disabled", projectSlug)
		}

		if s.defaultTeamSlug == "" {
			return nil, fmt.Errorf("no team available for creating project %s", projectSlug)
		}

		s.projectCreationMu.Lock()
		defer s.projectCreationMu.Unlock()

		s.projectMapMu.RLock()
		if cached, ok := s.projectToEndpoint[projectSlug]; ok {
			s.projectMapMu.RUnlock()
			return cached, nil
		}
		s.projectMapMu.RUnlock()

		if _, err := s.sentryClient.CreateProject(ctx, s.config.DynamicMode.OrgSlug, s.defaultTeamSlug, projectSlug, projectSlug, platform); err != nil {
			return nil, fmt.Errorf("failed to create project %s: %w", projectSlug, err)
		}

		endpoint, err := s.sentryClient.GetOTLPEndpoints(ctx, s.config.DynamicMode.OrgSlug, projectSlug)
		if err != nil {
			return nil, fmt.Errorf("failed to get endpoints for newly created project %s: %w", projectSlug, err)
		}

		s.projectMapMu.Lock()
		s.projectToEndpoint[projectSlug] = endpoint
		s.projectMapMu.Unlock()

		logger.Info("Successfully created project",
			zap.String("project", projectSlug),
			zap.String("team", s.defaultTeamSlug),
			zap.String("platform", platform),
		)

		return endpoint, nil
	})

	if err != nil {
		return nil, err
	}
	return result.(*OTLPEndpoints), nil
}
