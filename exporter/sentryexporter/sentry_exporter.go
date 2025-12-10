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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"go.uber.org/zap"
)

type sentryExporter struct {
	config *Config
	logger *zap.Logger
	client *http.Client

	dsnEndpoint *OTLPEndpoints

	sentryClient      SentryAPIClient
	projectToEndpoint map[string]*OTLPEndpoints
	projectMapMu      sync.RWMutex
	projectCreationMu sync.Mutex
	attributeKey      string
	projectMapping    map[string]string
	defaultTeamSlug   string
}

// pushTraceData takes an incoming OpenTelemetry trace, and forwards it to the Sentry OTLP endpoint.
func (e *sentryExporter) pushTraceData(ctx context.Context, td ptrace.Traces) error {
	if td.SpanCount() == 0 {
		return nil
	}

	if e.config.IsDSNMode() {
		return e.sendTracesToEndpoint(ctx, td, e.dsnEndpoint)
	}

	return e.routeTracesByProject(ctx, td)
}

// routeTracesByProject splits traces by project and sends each batch to the appropriate endpoint
func (e *sentryExporter) routeTracesByProject(ctx context.Context, td ptrace.Traces) error {
	type projectKey struct {
		slug     string
		platform string
	}
	projectGroups := make(map[projectKey]ptrace.Traces)

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		attrs := rs.Resource().Attributes()
		projectSlug := e.extractProjectSlug(attrs)

		if projectSlug == "" {
			e.logger.Warn("Dropping trace: missing required routing attribute",
				zap.String("attribute", e.attributeKey))
			continue
		}

		platform := e.extractPlatform(attrs)
		key := projectKey{slug: projectSlug, platform: platform}

		if _, exists := projectGroups[key]; !exists {
			projectGroups[key] = ptrace.NewTraces()
		}
		rs.CopyTo(projectGroups[key].ResourceSpans().AppendEmpty())
	}

	var errs error
	for key, traces := range projectGroups {
		endpoint, err := e.getOrCreateProjectEndpoint(ctx, key.slug, key.platform)
		if err != nil {
			e.logger.Error("Failed to get endpoint for project",
				zap.String("project", key.slug),
				zap.Error(err))
			errs = errors.Join(errs, err)
			continue
		}

		if err := e.sendTracesToEndpoint(ctx, traces, endpoint); err != nil {
			var httpErr *sentryHTTPError
			if errors.As(err, &httpErr) {
				if httpErr.statusCode == http.StatusForbidden && strings.Contains(httpErr.body, "event submission rejected with_reason: ProjectId") {
					e.logger.Warn("Project may have been deleted, removing from cache",
						zap.String("project", key.slug),
						zap.Int("status_code", httpErr.statusCode))
					e.projectMapMu.Lock()
					delete(e.projectToEndpoint, key.slug)
					e.projectMapMu.Unlock()
				}
			}

			e.logger.Error("Failed to send traces to project",
				zap.String("project", key.slug),
				zap.Error(err))
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

// sendTracesToEndpoint sends traces to a specific endpoint
func (e *sentryExporter) sendTracesToEndpoint(ctx context.Context, td ptrace.Traces, endpoint *OTLPEndpoints) error {
	request := ptraceotlp.NewExportRequestFromTraces(td)
	data, err := request.MarshalProto()
	if err != nil {
		return fmt.Errorf("failed to marshal traces: %w", err)
	}

	authHeader := fmt.Sprintf("sentry sentry_key=%s", endpoint.PublicKey)
	if err := e.sendOTLPData(ctx, endpoint.TracesURL, data, authHeader); err != nil {
		return fmt.Errorf("failed to send traces to Sentry: %w", err)
	}

	e.logger.Debug("Successfully sent traces to Sentry",
		zap.Int("span_count", td.SpanCount()))

	return nil
}

// Start starts the exporter
func (e *sentryExporter) Start(ctx context.Context, _ component.Host) error {
	if e.config.IsDSNMode() {
		e.logger.Info("Starting sentryexporter in DSN mode",
			zap.String("traces_endpoint", e.dsnEndpoint.TracesURL),
			zap.String("logs_endpoint", e.dsnEndpoint.LogsURL))
		return nil
	}

	e.logger.Info("Starting sentryexporter in dynamic mode",
		zap.String("org", e.config.DynamicMode.OrgSlug))

	projects, err := e.sentryClient.GetAllProjects(ctx, e.config.DynamicMode.OrgSlug)
	if err != nil {
		e.logger.Warn("Failed to pre-populate project cache",
			zap.Error(err))
		return nil
	}

	e.projectMapMu.Lock()
	for _, project := range projects {
		if e.defaultTeamSlug == "" && len(project.Teams) > 0 {
			e.defaultTeamSlug = project.Teams[0].Slug
		}

		endpoint, err := e.sentryClient.GetOTLPEndpoints(ctx, e.config.DynamicMode.OrgSlug, project.Slug)
		if err != nil {
			e.logger.Warn("Failed to fetch endpoint for project",
				zap.String("project", project.Slug),
				zap.Error(err))
			continue
		}
		e.projectToEndpoint[project.Slug] = endpoint
	}
	e.projectMapMu.Unlock()

	return nil
}

// Shutdown stops the exporter
func (e *sentryExporter) Shutdown(_ context.Context) error {
	e.logger.Info("Shutting down sentryexporter")
	if e.client != nil {
		e.client.CloseIdleConnections()
	}
	return nil
}

func (e *sentryExporter) pushLogData(ctx context.Context, ld plog.Logs) error {
	if ld.LogRecordCount() == 0 {
		return nil
	}

	if e.config.IsDSNMode() {
		return e.sendLogsToEndpoint(ctx, ld, e.dsnEndpoint)
	}

	return e.routeLogsByProject(ctx, ld)
}

func (e *sentryExporter) routeLogsByProject(ctx context.Context, ld plog.Logs) error {
	type projectKey struct {
		slug     string
		platform string
	}
	projectGroups := make(map[projectKey]plog.Logs)

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		attrs := rl.Resource().Attributes()
		projectSlug := e.extractProjectSlug(attrs)

		if projectSlug == "" {
			e.logger.Warn("Dropping logs: missing required routing attribute",
				zap.String("attribute", e.attributeKey))
			continue
		}

		platform := e.extractPlatform(attrs)
		key := projectKey{slug: projectSlug, platform: platform}

		if _, exists := projectGroups[key]; !exists {
			projectGroups[key] = plog.NewLogs()
		}
		rl.CopyTo(projectGroups[key].ResourceLogs().AppendEmpty())
	}

	var errs error
	for key, logs := range projectGroups {
		endpoint, err := e.getOrCreateProjectEndpoint(ctx, key.slug, key.platform)
		if err != nil {
			e.logger.Error("Failed to get endpoint for project",
				zap.String("project", key.slug),
				zap.Error(err))
			errs = errors.Join(errs, err)
			continue
		}

		if err := e.sendLogsToEndpoint(ctx, logs, endpoint); err != nil {
			var httpErr *sentryHTTPError
			if errors.As(err, &httpErr) {
				if httpErr.statusCode == http.StatusForbidden && strings.Contains(httpErr.body, "event submission rejected with_reason: ProjectId") {
					e.logger.Warn("Project may have been deleted, removing from cache",
						zap.String("project", key.slug),
						zap.Int("status_code", httpErr.statusCode))
					e.projectMapMu.Lock()
					delete(e.projectToEndpoint, key.slug)
					e.projectMapMu.Unlock()
				}
			}

			e.logger.Error("Failed to send logs to project",
				zap.String("project", key.slug),
				zap.Error(err))
			errs = errors.Join(errs, err)
		}
	}

	return errs
}

func (e *sentryExporter) sendLogsToEndpoint(ctx context.Context, ld plog.Logs, endpoint *OTLPEndpoints) error {
	request := plogotlp.NewExportRequestFromLogs(ld)
	data, err := request.MarshalProto()
	if err != nil {
		return fmt.Errorf("failed to marshal logs: %w", err)
	}

	authHeader := fmt.Sprintf("sentry sentry_key=%s", endpoint.PublicKey)
	if err := e.sendOTLPData(ctx, endpoint.LogsURL, data, authHeader); err != nil {
		return fmt.Errorf("failed to send logs to Sentry: %w", err)
	}

	e.logger.Debug("Successfully sent logs to Sentry",
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

func (e *sentryExporter) sendOTLPData(ctx context.Context, endpoint string, data []byte, authHeader string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("x-sentry-auth", authHeader)

	resp, err := e.client.Do(req)
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

	e.logger.Debug("Successfully sent data to Sentry",
		zap.Int("status_code", resp.StatusCode),
		zap.Int("data_size", len(data)))

	return nil
}

func (e *sentryExporter) extractProjectSlug(attrs pcommon.Map) string {
	attrValue, exists := attrs.Get(e.attributeKey)
	if !exists {
		return ""
	}

	serviceName := attrValue.Str()
	if serviceName == "" {
		return ""
	}

	if e.projectMapping != nil {
		if mappedSlug, ok := e.projectMapping[serviceName]; ok {
			return mappedSlug
		}
	}

	return serviceName
}

func (*sentryExporter) extractPlatform(attrs pcommon.Map) string {
	if lang, exists := attrs.Get("telemetry.sdk.language"); exists {
		return lang.Str()
	}
	return "other"
}

func (e *sentryExporter) getOrCreateProjectEndpoint(ctx context.Context, projectSlug, platform string) (*OTLPEndpoints, error) {
	e.projectMapMu.RLock()
	if cached, ok := e.projectToEndpoint[projectSlug]; ok {
		e.projectMapMu.RUnlock()
		return cached, nil
	}
	e.projectMapMu.RUnlock()

	endpoint, err := e.sentryClient.GetOTLPEndpoints(ctx, e.config.DynamicMode.OrgSlug, projectSlug)
	if err == nil {
		e.projectMapMu.Lock()
		e.projectToEndpoint[projectSlug] = endpoint
		e.projectMapMu.Unlock()
		return endpoint, nil
	}

	if !e.config.DynamicMode.Routing.AutoCreateProjects {
		return nil, fmt.Errorf("project %s not found and auto_create_projects is disabled", projectSlug)
	}

	if e.defaultTeamSlug == "" {
		return nil, fmt.Errorf("no team available for creating project %s", projectSlug)
	}

	e.projectCreationMu.Lock()
	defer e.projectCreationMu.Unlock()

	_, err = e.sentryClient.CreateProject(ctx, e.config.DynamicMode.OrgSlug, e.defaultTeamSlug, projectSlug, projectSlug, platform)
	if err != nil {
		return nil, fmt.Errorf("failed to create project %s: %w", projectSlug, err)
	}

	endpoint, err = e.sentryClient.GetOTLPEndpoints(ctx, e.config.DynamicMode.OrgSlug, projectSlug)
	if err != nil {
		return nil, fmt.Errorf("failed to get endpoints for newly created project %s: %w", projectSlug, err)
	}

	e.projectMapMu.Lock()
	e.projectToEndpoint[projectSlug] = endpoint
	e.projectMapMu.Unlock()

	e.logger.Info("Successfully created project",
		zap.String("project", projectSlug),
		zap.String("team", e.defaultTeamSlug),
		zap.String("platform", platform),
	)

	return endpoint, nil
}

// newSentryExporter creates a new Sentry OTLP proxy exporter
func newSentryExporter(config *Config, set exporter.Settings) (*sentryExporter, error) {
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

	exp := &sentryExporter{
		config:            config,
		logger:            set.Logger,
		client:            client,
		projectToEndpoint: make(map[string]*OTLPEndpoints),
	}

	switch {
	case config.IsDSNMode():
		endpoint, err := ParseDSN(config.DSNMode.DSN)
		if err != nil {
			return nil, fmt.Errorf("failed to parse DSN: %w", err)
		}
		exp.dsnEndpoint = endpoint
		set.Logger.Info("Configured in DSN mode", zap.String("dsn", config.DSNMode.DSN))

	case config.IsDynamicMode():
		exp.sentryClient = NewSentryClient(
			config.DynamicMode.URL,
			string(config.DynamicMode.AuthToken),
			client,
		)

		exp.attributeKey = config.DynamicMode.Routing.AttributeForProject
		if exp.attributeKey == "" {
			exp.attributeKey = DefaultAttributeForProject
		}

		exp.projectMapping = config.DynamicMode.Routing.ProjectMapping
		set.Logger.Info("Configured in dynamic mode",
			zap.String("org", config.DynamicMode.OrgSlug),
			zap.String("routing_attribute", exp.attributeKey),
			zap.Bool("auto_create_projects", config.DynamicMode.Routing.AutoCreateProjects))

	default:
		return nil, errors.New("exporter must be configured in either DSN or dynamic mode")
	}

	return exp, nil
}
