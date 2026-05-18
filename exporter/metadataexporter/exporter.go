// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadataexporter // import "go.opentelemetry.io/collector/exporter/metadataexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const unknownServiceName = "unknown_service"

type metadataExporter struct {
	cfg    *Config
	logger *zap.Logger
	client *http.Client
	now    func() time.Time

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	fingerprint string
	lastSent    time.Time
}

type pendingMetadata struct {
	key         string
	fingerprint string
	payload     serviceMetadata
}

type serviceMetadata struct {
	ServiceName           string `json:"serviceName"`
	ServiceVersion        string `json:"serviceVersion"`
	HostName              string `json:"hostName"`
	HostID                string `json:"hostId"`
	ProcessPID            int64  `json:"processPid"`
	ProcessCommand        string `json:"processCommand"`
	ProcessCommandLine    string `json:"processCommandLine"`
	ProcessExecutableName string `json:"processExecutableName"`
	ProcessRuntimeName    string `json:"processRuntimeName"`
	ProcessRuntimeVersion string `json:"processRuntimeVersion"`
	OSType                string `json:"osType"`
	TelemetrySDKLanguage  string `json:"telemetrySdkLanguage"`
	TelemetrySDKName      string `json:"telemetrySdkName"`
	TelemetrySDKVersion   string `json:"telemetrySdkVersion"`
	DeploymentEnvironment string `json:"deploymentEnvironment"`
	ContainerID           string `json:"containerId"`
	K8sNamespaceName      string `json:"k8sNamespaceName"`
	K8sPodName            string `json:"k8sPodName"`
	K8sDeploymentName     string `json:"k8sDeploymentName"`
	IsUnknownService      bool   `json:"isUnknownService"`
	LastSeenUnixSec       int64  `json:"lastSeenUnixSec"`
}

func newMetadataExporter(cfg *Config, set exporter.Settings) *metadataExporter {
	logger := set.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	return &metadataExporter{
		cfg:    cfg,
		logger: logger,
		client: &http.Client{},
		now:    time.Now,
		cache:  make(map[string]cacheEntry),
	}
}

func (e *metadataExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	pending := e.collectMetadata(td)
	if len(pending) == 0 {
		return nil
	}

	payload := make([]serviceMetadata, 0, len(pending))
	for _, item := range pending {
		payload = append(payload, item.payload)
	}

	if err := e.sendMetadata(ctx, payload); err != nil {
		e.logger.Error("failed to export service metadata", zap.Error(err), zap.Int("services", len(payload)))
		return err
	}

	e.commitPending(pending)
	e.logger.Info("sent service metadata", zap.Int("services", len(payload)), zap.String("endpoint", e.cfg.Endpoint))
	return nil
}

func (e *metadataExporter) collectMetadata(td ptrace.Traces) []pendingMetadata {
	resourceSpans := td.ResourceSpans()
	if resourceSpans.Len() == 0 {
		return nil
	}

	now := e.now()
	batch := make(map[string]pendingMetadata)

	e.mu.Lock()
	defer e.mu.Unlock()

	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		metadata := extractMetadata(rs.Resource(), now)
		key := dedupeKey(metadata)
		fingerprint := metadata.fingerprint()

		if existing, ok := batch[key]; ok {
			if existing.fingerprint != fingerprint {
				batch[key] = pendingMetadata{key: key, fingerprint: fingerprint, payload: metadata}
			}
			continue
		}

		if !e.shouldSendLocked(key, fingerprint, now) {
			continue
		}

		batch[key] = pendingMetadata{
			key:         key,
			fingerprint: fingerprint,
			payload:     metadata,
		}
	}

	result := make([]pendingMetadata, 0, len(batch))
	for _, item := range batch {
		result = append(result, item)
	}
	return result
}

func (e *metadataExporter) shouldSendLocked(key, fingerprint string, now time.Time) bool {
	entry, ok := e.cache[key]
	if !ok {
		return true
	}
	if entry.fingerprint != fingerprint {
		return true
	}
	return now.Sub(entry.lastSent) >= e.cfg.SendInterval
}

func (e *metadataExporter) commitPending(items []pendingMetadata) {
	now := e.now()

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, item := range items {
		e.cache[item.key] = cacheEntry{
			fingerprint: item.fingerprint,
			lastSent:    now,
		}
	}
}

func (e *metadataExporter) sendMetadata(ctx context.Context, payload []serviceMetadata) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal metadata payload: %w", err)
	}

	reqCtx, cancel := context.WithTimeout(ctx, e.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, e.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if e.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.cfg.APIKey)
	}

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("post metadata: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		msg, readErr := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if readErr != nil {
			return fmt.Errorf("backend returned status %d and body could not be read: %w", resp.StatusCode, readErr)
		}
		return fmt.Errorf("backend returned status %d: %s", resp.StatusCode, string(msg))
	}

	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	return nil
}

func extractMetadata(resource pcommon.Resource, now time.Time) serviceMetadata {
	attrs := resource.Attributes()

	serviceName := stringAttr(attrs, "service.name")
	if serviceName == "" {
		serviceName = stringAttr(attrs, "process.executable.name")
	}
	if serviceName == "" {
		serviceName = stringAttr(attrs, "process.command")
	}

	isUnknownService := false
	if serviceName == "" {
		serviceName = unknownServiceName
		isUnknownService = true
	}

	return serviceMetadata{
		ServiceName:           serviceName,
		ServiceVersion:        stringAttr(attrs, "service.version"),
		HostName:              stringAttr(attrs, "host.name"),
		HostID:                stringAttr(attrs, "host.id"),
		ProcessPID:            int64Attr(attrs, "process.pid"),
		ProcessCommand:        stringAttr(attrs, "process.command"),
		ProcessCommandLine:    stringAttr(attrs, "process.command_line"),
		ProcessExecutableName: stringAttr(attrs, "process.executable.name"),
		ProcessRuntimeName:    stringAttr(attrs, "process.runtime.name"),
		ProcessRuntimeVersion: stringAttr(attrs, "process.runtime.version"),
		OSType:                stringAttr(attrs, "os.type"),
		TelemetrySDKLanguage:  stringAttr(attrs, "telemetry.sdk.language"),
		TelemetrySDKName:      stringAttr(attrs, "telemetry.sdk.name"),
		TelemetrySDKVersion:   stringAttr(attrs, "telemetry.sdk.version"),
		DeploymentEnvironment: stringAttr(attrs, "deployment.environment.name"),
		ContainerID:           stringAttr(attrs, "container.id"),
		K8sNamespaceName:      stringAttr(attrs, "k8s.namespace.name"),
		K8sPodName:            stringAttr(attrs, "k8s.pod.name"),
		K8sDeploymentName:     stringAttr(attrs, "k8s.deployment.name"),
		IsUnknownService:      isUnknownService,
		LastSeenUnixSec:       now.Unix(),
	}
}

func dedupeKey(metadata serviceMetadata) string {
	return fmt.Sprintf("%s|%s|%d", metadata.HostName, metadata.ServiceName, metadata.ProcessPID)
}

func (m serviceMetadata) fingerprint() string {
	copy := m
	copy.LastSeenUnixSec = 0

	data, err := json.Marshal(copy)
	if err != nil {
		return fmt.Sprintf("%s|%d", m.ServiceName, m.ProcessPID)
	}
	return string(data)
}

func stringAttr(attrs pcommon.Map, key string) string {
	value, ok := attrs.Get(key)
	if !ok || value.Type() != pcommon.ValueTypeStr {
		return ""
	}
	return value.Str()
}

func int64Attr(attrs pcommon.Map, key string) int64 {
	value, ok := attrs.Get(key)
	if !ok || value.Type() != pcommon.ValueTypeInt {
		return 0
	}
	return value.Int()
}
