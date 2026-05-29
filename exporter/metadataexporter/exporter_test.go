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

func testExporter() *metadataExporter {
	return &metadataExporter{
		cfg: &Config{
			Endpoint:     "http://example.com",
			SendInterval: time.Minute,
			Timeout:      time.Second,
		},
		logger:   zap.NewNop(),
		now:      time.Now,
		serverIP: "192.0.2.1",
		cache:    make(map[string]cacheEntry),
	}
}

// --- Config validation ---

func TestConfigValidation(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{Endpoint: "http://example.com", SendInterval: time.Minute, Timeout: 5 * time.Second}
		assert.NoError(t, cfg.Validate())
	})

	t.Run("empty endpoint", func(t *testing.T) {
		cfg := &Config{SendInterval: time.Minute, Timeout: 5 * time.Second}
		assert.EqualError(t, cfg.Validate(), "endpoint is required")
	})

	t.Run("zero send_interval", func(t *testing.T) {
		cfg := &Config{Endpoint: "http://example.com", Timeout: 5 * time.Second}
		assert.EqualError(t, cfg.Validate(), "send_interval must be greater than zero")
	})

	t.Run("zero timeout", func(t *testing.T) {
		cfg := &Config{Endpoint: "http://example.com", SendInterval: time.Minute}
		assert.EqualError(t, cfg.Validate(), "timeout must be greater than zero")
	})
}

// --- Metadata extraction ---

func TestExtractMetadata(t *testing.T) {
	now := time.Unix(1710000000, 0)
	res := pcommon.NewResource()
	attrs := res.Attributes()
	attrs.PutStr("service.name", "payment-service")
	attrs.PutStr("service.version", "1.0.0")
	attrs.PutStr("host.name", "server-1")
	attrs.PutInt("process.pid", 1234)
	attrs.PutStr("process.command_line", "java -jar payment.jar")
	attrs.PutStr("process.executable.path", "/usr/bin/java")
	attrs.PutStr("process.owner", "appuser")
	attrs.PutStr("process.runtime.name", "OpenJDK Runtime Environment")
	attrs.PutStr("process.runtime.version", "17.0.10")
	attrs.PutStr("process.runtime.description", "Java(TM) SE Runtime Environment (build 17.0.10+7)")
	attrs.PutStr("os.type", "linux")
	attrs.PutStr("telemetry.sdk.language", "java")
	attrs.PutStr("telemetry.sdk.name", "opentelemetry")
	attrs.PutStr("telemetry.sdk.version", "1.35.0")
	attrs.PutStr("container.id", "container-123")

	got := testExporter().extractMetadata(res, now)

	assert.Equal(t, serviceMetadata{
		ServiceName:               "payment-service",
		ServiceVersion:            "1.0.0",
		HostName:                  "server-1",
		HostIP:                    "192.0.2.1",
		ProcessPID:                1234,
		ProcessCommandLine:        "java -jar payment.jar",
		ProcessExecutablePath:     "/usr/bin/java",
		ProcessOwner:              "appuser",
		ProcessRuntimeName:        "OpenJDK Runtime Environment",
		ProcessRuntimeVersion:     "17.0.10",
		ProcessRuntimeDescription: "Java(TM) SE Runtime Environment (build 17.0.10+7)",
		OSType:                    "linux",
		TelemetrySDKLanguage:      "java",
		TelemetrySDKName:          "opentelemetry",
		TelemetrySDKVersion:       "1.35.0",
		ContainerID:               "container-123",
		Timestamp:                 1710000000000,
	}, got)
}

func TestProcessCommandLineConsolidation(t *testing.T) {
	now := time.Unix(1710000000, 0)

	t.Run("prefers command_line over command_args", func(t *testing.T) {
		res := pcommon.NewResource()
		res.Attributes().PutStr("process.command_line", "java -jar app.jar")
		args := res.Attributes().PutEmptySlice("process.command_args")
		args.AppendEmpty().SetStr("java")
		args.AppendEmpty().SetStr("-jar")
		args.AppendEmpty().SetStr("app.jar")
		got := testExporter().extractMetadata(res, now)
		assert.Equal(t, "java -jar app.jar", got.ProcessCommandLine)
	})

	t.Run("falls back to joined command_args when command_line absent", func(t *testing.T) {
		res := pcommon.NewResource()
		args := res.Attributes().PutEmptySlice("process.command_args")
		args.AppendEmpty().SetStr("java")
		args.AppendEmpty().SetStr("-jar")
		args.AppendEmpty().SetStr("app.jar")
		got := testExporter().extractMetadata(res, now)
		assert.Equal(t, "java -jar app.jar", got.ProcessCommandLine)
	})

	t.Run("empty when neither set", func(t *testing.T) {
		res := pcommon.NewResource()
		got := testExporter().extractMetadata(res, now)
		assert.Equal(t, "", got.ProcessCommandLine)
	})
}

func TestFallbackServiceNameLogic(t *testing.T) {
	now := time.Unix(1710000000, 0)

	t.Run("uses service.name when present", func(t *testing.T) {
		res := pcommon.NewResource()
		res.Attributes().PutStr("service.name", "payment-service")
		got := testExporter().extractMetadata(res, now)
		assert.Equal(t, "payment-service", got.ServiceName)
	})

	t.Run("falls back to unknown_service:<lang> when language present", func(t *testing.T) {
		res := pcommon.NewResource()
		res.Attributes().PutStr("telemetry.sdk.language", "java")
		got := testExporter().extractMetadata(res, now)
		assert.Equal(t, "unknown_service:java", got.ServiceName)
	})

	t.Run("falls back to unknown_service when no name and no language", func(t *testing.T) {
		res := pcommon.NewResource()
		got := testExporter().extractMetadata(res, now)
		assert.Equal(t, unknownServiceName, got.ServiceName)
	})
}

// --- Deduplication and cache ---

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

	// immediately after commit — cached, skip
	second := exp.collectMetadata(td)
	assert.Empty(t, second)

	// 30s later — still within interval, skip
	fixed = fixed.Add(30 * time.Second)
	third := exp.collectMetadata(td)
	assert.Empty(t, third)

	// 61s later — interval expired, resend
	fixed = fixed.Add(31 * time.Second)
	fourth := exp.collectMetadata(td)
	require.Len(t, fourth, 1)
	exp.commitPending(fourth)

	// fingerprint changed — resend immediately regardless of interval
	tdChanged := tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
		attrs.PutStr("service.version", "2.0.0")
	})
	changed := exp.collectMetadata(tdChanged)
	require.Len(t, changed, 1)
}

func TestMultipleServicesInBatch(t *testing.T) {
	exp := testExporter()

	td := ptrace.NewTraces()

	rs1 := td.ResourceSpans().AppendEmpty()
	rs1.Resource().Attributes().PutStr("service.name", "service-a")
	rs1.Resource().Attributes().PutStr("host.name", "host-1")
	rs1.Resource().Attributes().PutInt("process.pid", 100)
	rs1.ScopeSpans().AppendEmpty().Spans().AppendEmpty()

	rs2 := td.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("service.name", "service-b")
	rs2.Resource().Attributes().PutStr("host.name", "host-1")
	rs2.Resource().Attributes().PutInt("process.pid", 200)
	rs2.ScopeSpans().AppendEmpty().Spans().AppendEmpty()

	pending := exp.collectMetadata(td)
	require.Len(t, pending, 2)

	names := make(map[string]bool)
	for _, p := range pending {
		names[p.payload.ServiceName] = true
	}
	assert.True(t, names["service-a"])
	assert.True(t, names["service-b"])
}

func TestSameServiceRepeatedInBatchCollapsedToOne(t *testing.T) {
	exp := testExporter()

	td := ptrace.NewTraces()
	for range 5 {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "payment-service")
		rs.Resource().Attributes().PutStr("host.name", "host-1")
		rs.Resource().Attributes().PutInt("process.pid", 1234)
		rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	}

	pending := exp.collectMetadata(td)
	require.Len(t, pending, 1)
	assert.Equal(t, "payment-service", pending[0].payload.ServiceName)
}

func TestCacheUpdatedAfterSuccessfulSend(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exp := newMetadataExporter(&Config{
		Endpoint:     server.URL,
		SendInterval: time.Minute,
		Timeout:      5 * time.Second,
	}, exporterSettings())

	td := tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
	})

	require.NoError(t, exp.pushTraces(context.Background(), td))

	// cache updated — second collect returns nothing
	pending := exp.collectMetadata(td)
	assert.Empty(t, pending)
}

// --- HTTP behaviour ---

func TestHTTPSuccessfulExport(t *testing.T) {
	var requests atomic.Int32
	var payload []serviceMetadata

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
		w.WriteHeader(http.StatusAccepted)
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
	require.NoError(t, err)
	require.Equal(t, int32(1), requests.Load())
	require.Len(t, payload, 1)
	assert.Equal(t, "payment-service", payload[0].ServiceName)
	assert.Equal(t, int64(1710000000000), payload[0].Timestamp)
}

func TestHTTPNon2xxFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "backend rejected request", http.StatusBadGateway)
	}))
	defer server.Close()

	exp := newMetadataExporter(&Config{
		Endpoint:     server.URL,
		SendInterval: time.Minute,
		Timeout:      5 * time.Second,
	}, exporterSettings())
	exp.now = func() time.Time { return time.Unix(1710000000, 0) }

	// best-effort: HTTP errors are logged but not returned to the pipeline
	require.NoError(t, exp.pushTraces(context.Background(), tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
	})))

	// cache must NOT have been updated on failure — next call should retry
	retry := exp.collectMetadata(tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
	}))
	require.Len(t, retry, 1)
}

func TestHTTPNetworkError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	url := server.URL
	server.Close() // close immediately so all connections are refused

	exp := newMetadataExporter(&Config{
		Endpoint:     url,
		SendInterval: time.Minute,
		Timeout:      5 * time.Second,
	}, exporterSettings())

	// best-effort: network errors are logged, not returned
	require.NoError(t, exp.pushTraces(context.Background(), tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
	})))

	// cache not updated — will retry on next call
	retry := exp.collectMetadata(tracesWithResource(func(attrs pcommon.Map) {
		attrs.PutStr("service.name", "payment-service")
		attrs.PutStr("host.name", "server-1")
		attrs.PutInt("process.pid", 1234)
	}))
	require.Len(t, retry, 1)
}

func TestEmptyResourceSpansHandling(t *testing.T) {
	var requests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	exp := newMetadataExporter(&Config{
		Endpoint:     server.URL,
		SendInterval: time.Minute,
		Timeout:      5 * time.Second,
	}, exporterSettings())

	require.NoError(t, exp.pushTraces(context.Background(), ptrace.NewTraces()))
	assert.Equal(t, int32(0), requests.Load())
}

// --- Helpers ---

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
	rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	return td
}
