// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sentryexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sentryexporter"

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
)

const (
	// DefaultAttributeForProject is the default resource attribute used for project routing
	DefaultAttributeForProject = "service.name"
)

type Config struct {
	URL                string              `mapstructure:"url"`
	OrgSlug            string              `mapstructure:"org_slug"`
	AuthToken          configopaque.String `mapstructure:"auth_token"`
	Routing            RoutingConfig       `mapstructure:"routing"`
	Timeout            time.Duration       `mapstructure:"timeout"`
	InsecureSkipVerify bool                `mapstructure:"insecure_skip_verify"`

	// Deprecated: Use OpenTelemetry resource attributes instead.
	Environment string `mapstructure:"environment"`

	confighttp.ClientConfig `mapstructure:",squash"`
}

type RoutingConfig struct {
	AutoCreateProjects  bool              `mapstructure:"auto_create_projects"`
	ProjectMapping      map[string]string `mapstructure:"project_mapping"`
	AttributeForProject string            `mapstructure:"attribute_for_project"`
}

func (cfg *Config) Validate() error {
	if cfg.URL == "" {
		return errors.New("'url' must be configured")
	}
	if _, err := url.Parse(cfg.URL); err != nil {
		return fmt.Errorf("invalid 'url': %w", err)
	}
	if cfg.OrgSlug == "" {
		return errors.New("'org_slug' is required")
	}
	if cfg.AuthToken == "" {
		return errors.New("'auth_token' is required")
	}
	if cfg.Timeout < 0 {
		return errors.New("'timeout' must be non-negative")
	}

	return nil
}

func (cfg *Config) GetTimeout() time.Duration {
	if cfg.Timeout == 0 {
		return 30 * time.Second
	}
	return cfg.Timeout
}

func (cfg *Config) GetInsecureSkipVerify() bool {
	return cfg.InsecureSkipVerify
}
