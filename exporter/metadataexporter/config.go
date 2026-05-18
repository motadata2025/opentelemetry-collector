// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadataexporter // import "go.opentelemetry.io/collector/exporter/metadataexporter"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

// Config defines configuration for the metadata exporter.
type Config struct {
	Endpoint     string        `mapstructure:"endpoint"`
	APIKey       string        `mapstructure:"api_key"`
	SendInterval time.Duration `mapstructure:"send_interval"`
	Timeout      time.Duration `mapstructure:"timeout"`

	// prevent unkeyed literal initialization
	_ struct{}
}

var _ component.Config = (*Config)(nil)

// Validate checks whether the exporter configuration is valid.
func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return errors.New("endpoint is required")
	}
	if cfg.SendInterval <= 0 {
		return errors.New("send_interval must be greater than zero")
	}
	if cfg.Timeout <= 0 {
		return errors.New("timeout must be greater than zero")
	}
	return nil
}
