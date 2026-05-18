// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadataexporter // import "go.opentelemetry.io/collector/exporter/metadataexporter"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/xexporter"
)

var componentType = component.MustNewType("metadata")

// NewFactory creates a factory for the metadata exporter.
func NewFactory() exporter.Factory {
	return xexporter.NewFactory(
		componentType,
		createDefaultConfig,
		xexporter.WithTraces(createTraces, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:     "http://localhost:8080/api/services/discovery",
		SendInterval: 60 * time.Second,
		Timeout:      5 * time.Second,
	}
}

func createTraces(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	exp := newMetadataExporter(cfg.(*Config), set)
	return exporterhelper.NewTraces(
		ctx,
		set,
		cfg,
		exp.pushTraces,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
	)
}
