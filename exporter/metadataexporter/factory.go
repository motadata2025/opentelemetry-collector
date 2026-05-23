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
	"go.opentelemetry.io/collector/exporter/metadataexporter/internal/metadata"
	"go.opentelemetry.io/collector/exporter/xexporter"
)

// NewFactory creates a factory for the metadata exporter.
func NewFactory() exporter.Factory {
	return xexporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		xexporter.WithTraces(createTraces, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
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
