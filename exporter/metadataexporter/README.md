# metadataexporter

`metadataexporter` is a custom trace-only OpenTelemetry Collector exporter that extracts lightweight service and runtime metadata from `ResourceSpans` and sends it to a discovery backend. It is intended for service discovery and application inventory UIs, not trace storage.

## Problem

Discovery backends often need service identity, version, host, process, runtime, and container metadata, but they do not need full spans. Shipping full traces to a discovery API increases cost, backend load, and privacy risk for no discovery benefit.

This exporter solves that by:

- reading only trace resources inside the Collector
- extracting only selected resource attributes
- deduplicating repeated metadata in memory
- posting compact JSON to a backend API

It never forwards span bodies to the discovery backend.

## Architecture

```text
Applications
  -> OpenTelemetry SDK / Agent
  -> OpenTelemetry Collector  (1 per server)
  -> metadataexporter
  -> Backend API /api/services/discovery
```

## Extracted Fields

The exporter extracts these resource attributes when present:

- `service.name`
- `service.version`
- `host.name`
- `process.pid`
- `process.command_line` (falls back to joining `process.command_args` if absent)
- `process.executable.path`
- `process.owner`
- `process.runtime.name`
- `process.runtime.version`
- `process.runtime.description`
- `os.type`
- `telemetry.sdk.language`
- `telemetry.sdk.name`
- `telemetry.sdk.version`
- `container.id`

In addition, `hostIp` is resolved automatically at Collector startup (see [Host IP Resolution](#host-ip-resolution)).

## Host IP Resolution

Because one Collector runs per server, the exporter resolves the server's own IP once at startup using a UDP dial:

```text
net.DialTimeout("udp", "8.8.8.8:80", 2s)
```

No data is actually sent. The OS routing table picks the correct outbound interface, so `hostIp` always reflects the primary server IP regardless of Docker bridges, VPN interfaces, or loopback addresses. The result is stored on the exporter and attached to every metadata payload.

If the dial fails (e.g. no network at startup), `hostIp` is omitted from the payload.

## Service Identity

Service identity resolution is:

1. `service.name`
2. `unknown_service:<telemetry.sdk.language>` (when `service.name` is absent but SDK language is present)
3. `unknown_service`

## Deduplication

Deduplication key:

```text
host.name + service.name + process.pid
```

The exporter re-sends metadata only when:

- a service is new
- any metadata field changed (fingerprint mismatch)
- the last send is older than `send_interval`

## Configuration

```yaml
exporters:
  metadata:
    endpoint: http://localhost:8080/api/services/discovery
    api_key: ""
    send_interval: 60s
    timeout: 5s
```

| Field | Default | Description |
|---|---|---|
| `endpoint` | _(required)_ | HTTP POST URL for the discovery backend |
| `api_key` | `""` | Bearer token sent in `Authorization` header |
| `send_interval` | `60s` | Minimum time between repeated sends for the same service |
| `timeout` | `5s` | HTTP request timeout |

## Example Collector Config

```yaml
receivers:
  otlp:
    protocols:
      grpc:
      http:

exporters:
  metadata:
    endpoint: http://localhost:8080/api/services/discovery
    send_interval: 60s
    timeout: 5s

  debug:
    verbosity: basic

processors:
  batch:

service:
  pipelines:
    traces/discovery:
      receivers: [otlp]
      processors: [batch]
      exporters: [metadata]
```

## Payload Format

```json
[
  {
    "serviceName": "payment-service",
    "serviceVersion": "1.0.0",
    "hostName": "server-1",
    "hostIp": "192.168.1.10",
    "processPid": 1234,
    "processCommandLine": "java -jar payment.jar",
    "processExecutablePath": "/usr/bin/java",
    "processOwner": "appuser",
    "processRuntimeName": "OpenJDK Runtime Environment",
    "processRuntimeVersion": "17.0.10",
    "processRuntimeDescription": "Java(TM) SE Runtime Environment (build 17.0.10+7)",
    "osType": "linux",
    "telemetrySdkLanguage": "java",
    "telemetrySdkName": "opentelemetry",
    "telemetrySdkVersion": "1.35.0",
    "containerId": "container-123",
    "timestamp": 1710000000
  }
]
```

`hostIp` is omitted if IP resolution failed at startup.

## Build

Build this exporter as part of a custom Collector with OCB:

```yaml
dist:
  module: go.opentelemetry.io/collector/cmd/otelcol-metadata
  name: otelcol-metadata
  description: Collector build with metadataexporter
  version: 0.143.0-dev

receivers:
  - gomod: go.opentelemetry.io/collector/receiver/otlpreceiver v0.143.0

exporters:
  - gomod: go.opentelemetry.io/collector/exporter/metadataexporter v0.143.0
  - gomod: go.opentelemetry.io/collector/exporter/debugexporter v0.143.0

processors:
  - gomod: go.opentelemetry.io/collector/processor/batchprocessor v0.143.0

providers:
  - gomod: go.opentelemetry.io/collector/confmap/provider/envprovider v1.49.0
  - gomod: go.opentelemetry.io/collector/confmap/provider/fileprovider v1.49.0
  - gomod: go.opentelemetry.io/collector/confmap/provider/yamlprovider v1.49.0

replaces:
  - go.opentelemetry.io/collector => ../../
  - go.opentelemetry.io/collector/component => ../../component
  - go.opentelemetry.io/collector/confmap => ../../confmap
  - go.opentelemetry.io/collector/consumer => ../../consumer
  - go.opentelemetry.io/collector/exporter => ../
  - go.opentelemetry.io/collector/exporter/metadataexporter => .
  - go.opentelemetry.io/collector/exporter/debugexporter => ../debugexporter
  - go.opentelemetry.io/collector/pdata => ../../pdata
  - go.opentelemetry.io/collector/processor/batchprocessor => ../../processor/batchprocessor
  - go.opentelemetry.io/collector/receiver/otlpreceiver => ../../receiver/otlpreceiver
```

## How To Test

```bash
cd exporter/metadataexporter
go test ./...
```

The unit tests cover:

- metadata extraction from `ResourceSpans`
- `hostIp` population from resolved server IP
- service name fallback behavior
- `process.command_line` fallback to joined `process.command_args`
- deduplication decisions
- successful HTTP export
- non-2xx backend failures
- empty `ResourceSpans` handling

## Limitations

- The deduplication cache is in-memory only and resets on Collector restart.
- The exporter consumes traces to read resource metadata but intentionally discards span data.
- The backend contract is fixed to a JSON array over HTTP POST.
- Retries and persistent queues are not enabled in this minimal implementation.

## Production Recommendations

- Deploy one Collector per server so `hostIp` correctly identifies the originating machine.
- Run this exporter in a dedicated traces pipeline for discovery traffic.
- Use a short backend timeout and a bounded `send_interval`.
- Put the discovery backend behind a load balancer and TLS terminator.
- Add Collector-level retry or queue behavior if backend availability is variable.
- Keep the backend idempotent because repeated sends can happen after restarts or metadata changes.
