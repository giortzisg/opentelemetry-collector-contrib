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
	DSNMode     *DSNModeConfig     `mapstructure:"dsn_mode"`
	DynamicMode *DynamicModeConfig `mapstructure:"dynamic_mode"`

	// Deprecated: Use OpenTelemetry resource attributes instead.
	Environment string `mapstructure:"environment"`

	confighttp.ClientConfig `mapstructure:",squash"`
}

type DSNModeConfig struct {
	DSN                string        `mapstructure:"dsn"`
	Timeout            time.Duration `mapstructure:"timeout"`
	InsecureSkipVerify bool          `mapstructure:"insecure_skip_verify"`
}

type DynamicModeConfig struct {
	URL                string              `mapstructure:"url"`
	OrgSlug            string              `mapstructure:"org_slug"`
	AuthToken          configopaque.String `mapstructure:"auth_token"`
	Routing            RoutingConfig       `mapstructure:"routing"`
	Timeout            time.Duration       `mapstructure:"timeout"`
	InsecureSkipVerify bool                `mapstructure:"insecure_skip_verify"`
}

type RoutingConfig struct {
	AutoCreateProjects  bool              `mapstructure:"auto_create_projects"`
	ProjectMapping      map[string]string `mapstructure:"project_mapping"`
	AttributeForProject string            `mapstructure:"attribute_for_project"`
}

func (cfg *Config) Validate() error {
	if cfg.DSNMode == nil && cfg.DynamicMode == nil {
		return errors.New("either 'dsn_mode' or 'dynamic_mode' must be configured")
	}

	if cfg.DSNMode != nil && cfg.DynamicMode != nil {
		return errors.New("cannot use 'dsn_mode' and 'dynamic_mode' together")
	}

	if cfg.DSNMode != nil {
		if cfg.DSNMode.DSN == "" {
			return errors.New("'dsn_mode.dsn' is required")
		}
		if _, err := url.Parse(cfg.DSNMode.DSN); err != nil {
			return fmt.Errorf("invalid 'dsn_mode.dsn': %w", err)
		}
		if cfg.DSNMode.Timeout < 0 {
			return errors.New("'dsn_mode.timeout' must be non-negative")
		}
	}

	if cfg.DynamicMode != nil {
		if cfg.DynamicMode.URL == "" {
			return errors.New("'dynamic_mode.url' is required")
		}
		if _, err := url.Parse(cfg.DynamicMode.URL); err != nil {
			return fmt.Errorf("invalid 'dynamic_mode.url': %w", err)
		}
		if cfg.DynamicMode.OrgSlug == "" {
			return errors.New("'dynamic_mode.org_slug' is required")
		}
		if cfg.DynamicMode.AuthToken == "" {
			return errors.New("'dynamic_mode.auth_token' is required")
		}
		if cfg.DynamicMode.Timeout < 0 {
			return errors.New("'dynamic_mode.timeout' must be non-negative")
		}
	}

	return nil
}

func (cfg *Config) IsDSNMode() bool {
	return cfg.DSNMode != nil
}

func (cfg *Config) IsDynamicMode() bool {
	return cfg.DynamicMode != nil
}

func (cfg *Config) GetTimeout() time.Duration {
	if cfg.DSNMode != nil {
		if cfg.DSNMode.Timeout == 0 {
			return 30 * time.Second
		}
		return cfg.DSNMode.Timeout
	}
	if cfg.DynamicMode != nil {
		if cfg.DynamicMode.Timeout == 0 {
			return 30 * time.Second
		}
		return cfg.DynamicMode.Timeout
	}
	return 30 * time.Second
}

func (cfg *Config) GetInsecureSkipVerify() bool {
	if cfg.DSNMode != nil {
		return cfg.DSNMode.InsecureSkipVerify
	}
	if cfg.DynamicMode != nil {
		return cfg.DynamicMode.InsecureSkipVerify
	}
	return false
}
