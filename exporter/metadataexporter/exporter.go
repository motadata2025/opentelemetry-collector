// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadataexporter // import "go.opentelemetry.io/collector/exporter/metadataexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const unknownServiceName = "unknown_service"

type metadataExporter struct {
	cfg      *Config
	logger   *zap.Logger
	client   *http.Client
	now      func() time.Time
	serverIP string

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
	ServiceName               string `json:"serviceName"`
	ServiceVersion            string `json:"serviceVersion"`
	HostName                  string `json:"hostName"`
	HostIP                    string `json:"hostIp,omitempty"`
	ProcessPID                int64  `json:"processPid"`
	ProcessCommandLine        string `json:"processCommandLine"`
	ProcessExecutablePath     string `json:"processExecutablePath"`
	ProcessOwner              string `json:"processOwner"`
	ProcessRuntimeName        string `json:"processRuntimeName"`
	ProcessRuntimeVersion     string `json:"processRuntimeVersion"`
	ProcessRuntimeDescription string `json:"processRuntimeDescription"`
	OSType                    string `json:"osType"`
	TelemetrySDKLanguage      string `json:"telemetrySdkLanguage"`
	TelemetrySDKName          string `json:"telemetrySdkName"`
	TelemetrySDKVersion       string `json:"telemetrySdkVersion"`
	ContainerID               string `json:"containerId"`
	Timestamp                 int64  `json:"timestamp"`
}

func newMetadataExporter(cfg *Config, set exporter.Settings) *metadataExporter {
	logger := set.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	return &metadataExporter{
		cfg:      cfg,
		logger:   logger,
		client:   &http.Client{},
		now:      time.Now,
		serverIP: resolveServerIP(),
		cache:    make(map[string]cacheEntry),
	}
}

// resolveServerIP returns the primary outbound IP by asking the OS routing
// table via a UDP dial — no data is actually sent, but the kernel picks the
// correct source interface the same way it would for real outbound traffic.
func resolveServerIP() string {
	conn, err := net.DialTimeout("udp", "8.8.8.8:80", 2*time.Second)
	if err != nil {
		return ""
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
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
		return nil
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

	for i := 0; i < resourceSpans.Len(); i++ {
		rs := resourceSpans.At(i)
		metadata := e.extractMetadata(rs.Resource(), now)
		key := dedupeKey(metadata)
		fingerprint := metadata.fingerprint()

		if existing, ok := batch[key]; ok {
			if existing.fingerprint != fingerprint {
				batch[key] = pendingMetadata{key: key, fingerprint: fingerprint, payload: metadata}
			}
			continue
		}

		e.mu.Lock()
		shouldSend := e.shouldSendLocked(key, fingerprint, now)
		e.mu.Unlock()

		if !shouldSend {
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

func (e *metadataExporter) extractMetadata(resource pcommon.Resource, now time.Time) serviceMetadata {
	attrs := resource.Attributes()

	serviceName := stringAttr(attrs, "service.name")
	if serviceName == "" {
		lang := stringAttr(attrs, "telemetry.sdk.language")
		if lang != "" {
			serviceName = unknownServiceName + ":" + lang
		} else {
			serviceName = unknownServiceName
		}
	}

	cmdLine := stringAttr(attrs, "process.command_line")
	if cmdLine == "" {
		if args := stringSliceAttr(attrs, "process.command_args"); len(args) > 0 {
			cmdLine = strings.Join(args, " ")
		}
	}

	return serviceMetadata{
		ServiceName:               serviceName,
		ServiceVersion:            stringAttr(attrs, "service.version"),
		HostName:                  stringAttr(attrs, "host.name"),
		HostIP:                    e.serverIP,
		ProcessPID:                int64Attr(attrs, "process.pid"),
		ProcessCommandLine:        cmdLine,
		ProcessExecutablePath:     stringAttr(attrs, "process.executable.path"),
		ProcessOwner:              stringAttr(attrs, "process.owner"),
		ProcessRuntimeName:        stringAttr(attrs, "process.runtime.name"),
		ProcessRuntimeVersion:     stringAttr(attrs, "process.runtime.version"),
		ProcessRuntimeDescription: stringAttr(attrs, "process.runtime.description"),
		OSType:                    stringAttr(attrs, "os.type"),
		TelemetrySDKLanguage:      stringAttr(attrs, "telemetry.sdk.language"),
		TelemetrySDKName:          stringAttr(attrs, "telemetry.sdk.name"),
		TelemetrySDKVersion:       stringAttr(attrs, "telemetry.sdk.version"),
		ContainerID:               stringAttr(attrs, "container.id"),
		Timestamp:                 now.UnixMilli(),
	}
}

func dedupeKey(metadata serviceMetadata) string {
	return fmt.Sprintf("%s|%s|%d", metadata.HostName, metadata.ServiceName, metadata.ProcessPID)
}

func (m serviceMetadata) fingerprint() string {
	snapshot := m
	snapshot.Timestamp = 0

	data, err := json.Marshal(snapshot)
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

func stringSliceAttr(attrs pcommon.Map, key string) []string {
	value, ok := attrs.Get(key)
	if !ok || value.Type() != pcommon.ValueTypeSlice {
		return nil
	}
	s := value.Slice()
	result := make([]string, 0, s.Len())
	for i := 0; i < s.Len(); i++ {
		result = append(result, s.At(i).Str())
	}
	return result
}
