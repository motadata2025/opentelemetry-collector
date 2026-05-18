# metadataexporter

`metadataexporter` is a custom trace-only OpenTelemetry Collector exporter that extracts lightweight service and runtime metadata from `ResourceSpans` and sends it to a discovery backend. It is intended for service discovery and application inventory UIs, not trace storage.

## Problem

Discovery backends often need service identity, version, host, process, runtime, container, and Kubernetes metadata, but they do not need full spans. Shipping full traces to a discovery API increases cost, backend load, and privacy risk for no discovery benefit.

This exporter solves that by:

- reading only trace resources inside the Collector
- extracting only selected resource attributes
- deduplicating repeated metadata in memory
- posting compact JSON to a backend API

It never forwards span bodies to the discovery backend.

## Architecture

```text
Java Applications
  -> OpenTelemetry Java Agent
  -> OpenTelemetry Collector
  -> metadataexporter
  -> Backend API /api/services/discovery
```

## Extracted Fields

The exporter extracts these resource attributes when present:

- `service.name`
- `service.version`
- `host.name`
- `host.id`
- `process.pid`
- `process.command`
- `process.command_line`
- `process.executable.name`
- `process.runtime.name`
- `process.runtime.version`
- `os.type`
- `telemetry.sdk.language`
- `telemetry.sdk.name`
- `telemetry.sdk.version`
- `deployment.environment.name`
- `container.id`
- `k8s.namespace.name`
- `k8s.pod.name`
- `k8s.deployment.name`

## Service Identity

Service identity resolution is:

1. `service.name`
2. `process.executable.name`
3. `process.command`
4. `unknown_service`

When the final fallback is used, the payload sets `isUnknownService: true`.

Deduplication uses:

```text
host.name + service.name + process.pid
```

The exporter re-sends metadata only when:

- a service is new
- metadata changed
- the last send time is older than `send_interval`

## Configuration

```yaml
exporters:
  metadata:
    endpoint: http://localhost:8080/api/services/discovery
    api_key: ""
    send_interval: 60s
    timeout: 5s
```

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
    "hostId": "host-abc",
    "processPid": 1234,
    "processCommand": "java",
    "processCommandLine": "java -jar payment.jar",
    "processExecutableName": "java",
    "processRuntimeName": "OpenJDK Runtime Environment",
    "processRuntimeVersion": "17.0.10",
    "osType": "linux",
    "telemetrySdkLanguage": "java",
    "telemetrySdkName": "opentelemetry",
    "telemetrySdkVersion": "1.35.0",
    "deploymentEnvironment": "prod",
    "containerId": "",
    "k8sNamespaceName": "",
    "k8sPodName": "",
    "k8sDeploymentName": "",
    "isUnknownService": false,
    "lastSeenUnixSec": 1710000000
  }
]
```

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

Run:

```bash
cd exporter/metadataexporter
go test ./...
```

The unit tests cover:

- metadata extraction from `ResourceSpans`
- service name fallback behavior
- deduplication decisions
- successful HTTP export
- non-2xx backend failures
- empty `ResourceSpans` handling

## Limitations

- The deduplication cache is in-memory only and resets on Collector restart.
- The exporter consumes traces to read resource metadata, but it intentionally discards span data for discovery export.
- The backend contract is fixed to a JSON array over HTTP POST.
- Retries and persistent queues are not enabled in this minimal implementation.

## Production Recommendations

- Run this exporter in a dedicated traces pipeline for discovery traffic.
- Use a short backend timeout and a bounded `send_interval`.
- Put the discovery backend behind a load balancer and TLS terminator.
- Add Collector-level retry or queue behavior if backend availability is variable.
- Keep the backend idempotent because repeated sends can happen after restarts or metadata changes.
