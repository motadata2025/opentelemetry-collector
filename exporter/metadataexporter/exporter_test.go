// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package metadataexporter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestExtractMetadata(t *testing.T) {
	now := time.Unix(1710000000, 0)
	res := pcommon.NewResource()
	attrs := res.Attributes()
	attrs.PutStr("service.name", "payment-service")
	attrs.PutStr("service.version", "1.0.0")
	attrs.PutStr("host.name", "server-1")
	attrs.PutStr("host.id", "host-abc")
	attrs.PutInt("process.pid", 1234)
	attrs.PutStr("process.command", "java")
	attrs.PutStr("process.command_line", "java -jar payment.jar")
	attrs.PutStr("process.executable.name", "java")
	attrs.PutStr("process.runtime.name", "OpenJDK Runtime Environment")
	attrs.PutStr("process.runtime.version", "17.0.10")
	attrs.PutStr("os.type", "linux")
	attrs.PutStr("telemetry.sdk.language", "java")
	attrs.PutStr("telemetry.sdk.name", "opentelemetry")
	attrs.PutStr("telemetry.sdk.version", "1.35.0")
	attrs.PutStr("deployment.environment.name", "prod")
	attrs.PutStr("container.id", "container-123")
	attrs.PutStr("k8s.namespace.name", "payments")
	attrs.PutStr("k8s.pod.name", "payment-7d4c")
	attrs.PutStr("k8s.deployment.name", "payment")

	got := extractMetadata(res, now)

	assert.Equal(t, serviceMetadata{
		ServiceName:           "payment-service",
		ServiceVersion:        "1.0.0",
		HostName:              "server-1",
		HostID:                "host-abc",
		ProcessPID:            1234,
		ProcessCommand:        "java",
		ProcessCommandLine:    "java -jar payment.jar",
		ProcessExecutableName: "java",
		ProcessRuntimeName:    "OpenJDK Runtime Environment",
		ProcessRuntimeVersion: "17.0.10",
		OSType:                "linux",
		TelemetrySDKLanguage:  "java",
		TelemetrySDKName:      "opentelemetry",
		TelemetrySDKVersion:   "1.35.0",
		DeploymentEnvironment: "prod",
		ContainerID:           "container-123",
		K8sNamespaceName:      "payments",
		K8sPodName:            "payment-7d4c",
		K8sDeploymentName:     "payment",
		IsUnknownService:      false,
		LastSeenUnixSec:       1710000000,
	}, got)
}

func TestFallbackServiceNameLogic(t *testing.T) {
	now := time.Unix(1710000000, 0)

	t.Run("uses process executable name", func(t *testing.T) {
		res := pcommon.NewResource()
		res.Attributes().PutStr("process.executable.name", "java")
		got := extractMetadata(res, now)
		assert.Equal(t, "java", got.ServiceName)
		assert.False(t, got.IsUnknownService)
	})

	t.Run("uses process command", func(t *testing.T) {
		res := pcommon.NewResource()
		res.Attributes().PutStr("process.command", "java")
		got := extractMetadata(res, now)
		assert.Equal(t, "java", got.ServiceName)
		assert.False(t, got.IsUnknownService)
	})

	t.Run("uses unknown service fallback", func(t *testing.T) {
		res := pcommon.NewResource()
		got := extractMetadata(res, now)
		assert.Equal(t, unknownServiceName, got.ServiceName)
		assert.True(t, got.IsUnknownService)
	})
}

func TestDeduplicationLogic(t *testing.T) {
	fixed := time.Unix(1710000000, 0)
	exp := &metadataExporter{
		cfg: &Config{
			Endpoint:     "http://example.com",
			SendInterval: time.Minute,
			Timeout:      time.Second,
		},
		logger: zap.NewNop(),
		client: &http.Client{},
		now:    func() time.Time { return fixed },
		cache:  make(map[string]cacheEntry),
	}

	td := tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
	})

	first := exp.collectMetadata(td)
	require.Len(t, first, 1)
	exp.commitPending(first)

	second := exp.collectMetadata(td)
	assert.Empty(t, second)

	fixed = fixed.Add(30 * time.Second)
	third := exp.collectMetadata(td)
	assert.Empty(t, third)

	fixed = fixed.Add(31 * time.Second)
	fourth := exp.collectMetadata(td)
	require.Len(t, fourth, 1)

	exp.commitPending(fourth)

	tdChanged := tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
		attrs.PutStr("service.version", "2.0.0")
	})
	changed := exp.collectMetadata(tdChanged)
	require.Len(t, changed, 1)
}

func TestHTTPSuccessfulExport(t *testing.T) {
	var requests atomic.Int32
	var authHeader string
	var payload []serviceMetadata

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		authHeader = r.Header.Get("Authorization")
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	exp := newMetadataExporter(&Config{
		Endpoint:     server.URL,
		APIKey:       "secret",
		SendInterval: time.Minute,
		Timeout:      5 * time.Second,
	}, exporterSettings())
	exp.now = func() time.Time { return time.Unix(1710000000, 0) }

	err := exp.pushTraces(context.Background(), tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
		attrs.PutStr("process.command", "java")
	}))
	require.NoError(t, err)
	require.Equal(t, int32(1), requests.Load())
	require.Equal(t, "Bearer secret", authHeader)
	require.Len(t, payload, 1)
	assert.Equal(t, "payment-service", payload[0].ServiceName)
	assert.Equal(t, int64(1710000000), payload[0].LastSeenUnixSec)
}

func TestHTTPNon2xxFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "backend rejected request", http.StatusBadGateway)
	}))
	defer server.Close()

	exp := newMetadataExporter(&Config{
		Endpoint:     server.URL,
		SendInterval: time.Minute,
		Timeout:      5 * time.Second,
	}, exporterSettings())
	exp.now = func() time.Time { return time.Unix(1710000000, 0) }

	err := exp.pushTraces(context.Background(), tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
	}))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "502")

	retry := exp.collectMetadata(tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
	}))
	require.Len(t, retry, 1)
}

func TestEmptyResourceSpansHandling(t *testing.T) {
	var requests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exp := newMetadataExporter(&Config{
		Endpoint:     server.URL,
		SendInterval: time.Minute,
		Timeout:      5 * time.Second,
	}, exporterSettings())

	err := exp.pushTraces(context.Background(), ptrace.NewTraces())
	require.NoError(t, err)
	assert.Equal(t, int32(0), requests.Load())
}

func exporterSettings() exporter.Settings {
	return exporter.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: zap.NewNop(),
		},
	}
}

func tracesWithResource(fill func(attrs pcommon.Map)) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	fill(rs.Resource().Attributes())
	scopeSpans := rs.ScopeSpans().AppendEmpty()
	scopeSpans.Spans().AppendEmpty()
	return td
}
