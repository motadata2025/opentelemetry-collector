// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlphttpexporter // import "go.opentelemetry.io/collector/exporter/otlphttpexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/internal/statusutil"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/pprofile/pprofileotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

type baseExporter struct {
	// Input configuration.
	config      *Config
	client      *http.Client
	tracesURL   string
	metricsURL  string
	logsURL     string
	profilesURL string
	logger      *zap.Logger
	settings    component.TelemetrySettings
	// Default user-agent header.
	userAgent string

	traceConfig        traceAgentConfig
	cancelConfigReader context.CancelFunc
}

type ServiceDetail struct {
	ID                int64  `json:"id"`
	TraceServiceName  string `json:"trace.service.name"`
	TraceServiceType  string `json:"trace.service.type"`
	ServiceTraceState string `json:"service.trace.state"`
}

type config interface {
	getTraceStatus() bool
	getAgentServiceStatus() bool
	getServiceState(service string) bool
	getServices() map[string]ServiceDetail
}

type agentConfig struct {
	Agent struct {
		TraceAgentStatus   string `json:"trace.agent.status"`
		AgentServiceStatus string `json:"agent.service.status"`
	} `json:"agent"`
	TraceAgent map[string]struct {
		ServiceTraceState string `json:"service.trace.state"`
	} `json:"trace.agent"`
}

type serviceConfig struct {
	TraceStatus string                   `json:"trace.status"`
	Services    map[string]ServiceDetail `json:"services"`
}

func (sConfig *serviceConfig) getTraceStatus() bool {
	return sConfig.TraceStatus == "yes"
}

func (sConfig *serviceConfig) getAgentServiceStatus() bool {
	return true
}

func (sConfig *serviceConfig) getServiceState(service string) bool {
	if val, ok := sConfig.Services[service]; ok {
		return val.ServiceTraceState == "yes"
	}
	return false
}

func (sConfig *serviceConfig) getServices() map[string]ServiceDetail {
	return sConfig.Services
}

func (aConfig *agentConfig) getTraceStatus() bool {
	return aConfig.Agent.TraceAgentStatus == "yes"
}

func (aConfig *agentConfig) getAgentServiceStatus() bool {
	return aConfig.Agent.AgentServiceStatus == "Running"
}

func (aConfig *agentConfig) getServiceState(service string) bool {
	if val, ok := aConfig.TraceAgent[service]; ok {
		return val.ServiceTraceState == "yes"
	}
	return false
}

func (aConfig *agentConfig) getServices() map[string]ServiceDetail {
	services := make(map[string]ServiceDetail)
	for name, details := range aConfig.TraceAgent {
		services[name] = ServiceDetail{
			ServiceTraceState: details.ServiceTraceState,
			// Other fields are not available in agentConfig, so they will be zero-valued
		}
	}
	return services
}

type traceAgentConfig struct {
	mu               sync.RWMutex
	serviceStatusMap map[string]bool
}

const (
	headerRetryAfter         = "Retry-After"
	maxHTTPResponseReadBytes = 64 * 1024

	jsonContentType     = "application/json"
	protobufContentType = "application/x-protobuf"
)

// createFileLogger creates a zap logger that writes to a file
func createFileLogger(logFilePath string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{logFilePath}
	cfg.ErrorOutputPaths = []string{logFilePath}

	return cfg.Build()
}

// Create new exporter.
func newExporter(cfg component.Config, set exporter.Settings) (*baseExporter, error) {
	oCfg := cfg.(*Config)

	if oCfg.ClientConfig.Endpoint != "" {
		_, err := url.Parse(oCfg.ClientConfig.Endpoint)
		if err != nil {
			return nil, errors.New("endpoint must be a valid URL")
		}
	}

	userAgent := fmt.Sprintf("%s/%s (%s/%s)",
		set.BuildInfo.Description, set.BuildInfo.Version, runtime.GOOS, runtime.GOARCH)

	// Create file logger instead of using console logger
	fileLogger, err := createFileLogger("/motadata/motadata/collector-log/otlp-exporter.log")
	if err != nil {
		// Fall back to the default logger if file logger creation fails
		fileLogger = set.Logger
	}

	// client construction is deferred to start
	return &baseExporter{
		config:    oCfg,
		logger:    fileLogger,
		userAgent: userAgent,
		settings:  set.TelemetrySettings,
	}, nil
}

// start actually creates the HTTP client. The client construction is deferred till this point as this
// is the only place we get hold of Extensions which are required to construct auth round tripper.
func (e *baseExporter) start(ctx context.Context, host component.Host) error {
	client, err := e.config.ClientConfig.ToClient(ctx, host, e.settings)
	if err != nil {
		return err
	}
	e.client = client

	// Start periodic config reader
	configCtx, cancel := context.WithCancel(context.Background())
	e.cancelConfigReader = cancel
	go e.periodicConfigReader(configCtx, 30*time.Second)
	return nil
}

func (e *baseExporter) periodicConfigReader(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial read
	e.readAgentConfig()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.readAgentConfig()
		}
	}
}

func (e *baseExporter) readAgentConfig() {
	currentDir, err := os.Getwd()
	if err != nil {
		e.logger.Error("Failed to get current directory", zap.Error(err))
		return
	}

	config, configPath, err := getConfigPath()
	if err != nil {
		e.logger.Error("Failed to get config path: ", zap.String("config_path", configPath), zap.Error(err))
		return
	}

	e.logger.Info("Current Dir is :", zap.String("currentDir", currentDir))

	data, err := os.ReadFile(configPath)
	if err != nil {
		e.logger.Error("Failed to read config file", zap.Error(err))
		return
	}

	if err := json.Unmarshal(data, config); err != nil {
		e.logger.Error("Failed to unmarshal config file", zap.Error(err))
		return
	}

	serviceMap := make(map[string]bool)
	traceAgentActive := config.getTraceStatus()
	agentRunning := config.getAgentServiceStatus()
	services := config.getServices()

	for serviceName, serviceDetail := range services {
		serviceMap[serviceName] = serviceDetail.ServiceTraceState == "yes" && traceAgentActive && agentRunning
	}

	e.traceConfig.mu.Lock()
	e.traceConfig.serviceStatusMap = serviceMap
	e.logger.Info("Trace On/Off debug ", zap.Any("config", e.traceConfig.serviceStatusMap))
	e.traceConfig.mu.Unlock()
}

func getConfigPath() (config, string, error) {
	currentDir, err := os.Getwd()
	if err != nil {
		return nil, "", err
	}

	// Check for trace-services.json first
	configPath := filepath.Join(currentDir, "config", "trace-services.json")
	_, err = os.Stat(configPath)
	if err == nil {
		return &serviceConfig{}, configPath, nil
	}

	// Check for agent.json if trace-services.json is not found
	configPath = filepath.Join(currentDir, "config", "agent.json")
	_, err = os.Stat(configPath)
	if err == nil {
		return &agentConfig{}, configPath, nil
	}

	return nil, "", fmt.Errorf("no valid config file found")
}

func getFirstServiceName(td ptrace.Traces) string {
	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		if val, ok := resourceSpans.At(i).Resource().Attributes().Get("service.name"); ok {
			return val.Str()
		}
	}
	return ""
}

func (e *baseExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {
	serviceName := getFirstServiceName(td)
	var request []byte
	if len(serviceName) > 0 && e.traceConfig.serviceStatusMap[serviceName] == true {
		var err error
		req := ptraceotlp.NewExportRequestFromTraces(td)

		switch e.config.Encoding {
		case EncodingJSON:
			request, err = req.MarshalJSON()
		case EncodingProto:
			request, err = req.MarshalProto()
		default:
			err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
		}

		if err != nil {
			e.logger.Error("failed to marshal trace data: ", zap.Error(err))
		}
		return e.export(ctx, e.tracesURL, request, e.tracesPartialSuccessHandler)
	}

	e.logger.Info("skipping trace data: service trace collection are off", zap.String("serviceName", serviceName))
	return nil
}

func getFirstServiceNameFromMetrics(md pmetric.Metrics) string {
	resourceMetrics := md.ResourceMetrics()
	for i := 0; i < resourceMetrics.Len(); i++ {
		if val, ok := resourceMetrics.At(i).Resource().Attributes().Get("service.name"); ok {
			return val.Str()
		}
	}
	return ""
}

func (e *baseExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	serviceName := getFirstServiceNameFromMetrics(md)
	var request []byte
	if len(serviceName) > 0 && e.traceConfig.serviceStatusMap[serviceName] == true {
		tr := pmetricotlp.NewExportRequestFromMetrics(md)
		var err error

		switch e.config.Encoding {
		case EncodingJSON:
			request, err = tr.MarshalJSON()
		case EncodingProto:
			request, err = tr.MarshalProto()
		default:
			err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
		}

		if err != nil {
			e.logger.Error("failed to marshal metrics data: ", zap.Error(err))
			return nil
		}

		return e.export(ctx, e.metricsURL, request, e.metricsPartialSuccessHandler)
	}

	e.logger.Info("skipping metrics data: service metric collection is off", zap.String("serviceName", serviceName))
	return nil
}

func (e *baseExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	tr := plogotlp.NewExportRequestFromLogs(ld)

	var err error
	var request []byte
	switch e.config.Encoding {
	case EncodingJSON:
		request, err = tr.MarshalJSON()
	case EncodingProto:
		request, err = tr.MarshalProto()
	default:
		err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	if err != nil {
		return consumererror.NewPermanent(err)
	}

	return e.export(ctx, e.logsURL, request, e.logsPartialSuccessHandler)
}

func (e *baseExporter) pushProfiles(ctx context.Context, td pprofile.Profiles) error {
	tr := pprofileotlp.NewExportRequestFromProfiles(td)

	var err error
	var request []byte
	switch e.config.Encoding {
	case EncodingJSON:
		request, err = tr.MarshalJSON()
	case EncodingProto:
		request, err = tr.MarshalProto()
	default:
		err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	if err != nil {
		return consumererror.NewPermanent(err)
	}

	return e.export(ctx, e.profilesURL, request, e.profilesPartialSuccessHandler)
}

func (e *baseExporter) export(ctx context.Context, url string, request []byte, partialSuccessHandler partialSuccessHandler) error {
	e.logger.Debug("Preparing to make HTTP request", zap.String("url", url))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(request))
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	switch e.config.Encoding {
	case EncodingJSON:
		req.Header.Set("Content-Type", jsonContentType)
	case EncodingProto:
		req.Header.Set("Content-Type", protobufContentType)
	default:
		return fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	req.Header.Set("User-Agent", e.userAgent)

	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to make an HTTP request: %w", err)
	}

	defer func() {
		// Discard any remaining response body when we are done reading.
		_, _ = io.CopyN(io.Discard, resp.Body, maxHTTPResponseReadBytes)
		resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return handlePartialSuccessResponse(resp, partialSuccessHandler)
	}

	respStatus := readResponseStatus(resp)

	// Format the error message. Use the status if it is present in the response.
	var errString string
	var formattedErr error
	if respStatus != nil {
		errString = fmt.Sprintf(
			"error exporting items, request to %s responded with HTTP Status Code %d, Message=%s, Details=%v",
			url, resp.StatusCode, respStatus.Message, respStatus.Details)
	} else {
		errString = fmt.Sprintf(
			"error exporting items, request to %s responded with HTTP Status Code %d",
			url, resp.StatusCode)
	}
	formattedErr = statusutil.NewStatusFromMsgAndHTTPCode(errString, resp.StatusCode).Err()

	if !isRetryableStatusCode(resp.StatusCode) {
		return consumererror.NewPermanent(formattedErr)
	}

	// Check if the server is overwhelmed.
	// See spec https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#otlphttp-throttling
	isThrottleError := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable
	if isThrottleError {
		// Use Values to check if the header is present, and if present even if it is empty return ThrottleRetry.
		values := resp.Header.Values(headerRetryAfter)
		if len(values) == 0 {
			return formattedErr
		}
		// The value of Retry-After field can be either an HTTP-date or a number of
		// seconds to delay after the response is received. See https://datatracker.ietf.org/doc/html/rfc7231#section-7.1.3
		//
		// Retry-After = HTTP-date / delay-seconds
		//
		// First try to parse delay-seconds, since that is what the receiver will send.
		if seconds, err := strconv.Atoi(values[0]); err == nil {
			return exporterhelper.NewThrottleRetry(formattedErr, time.Duration(seconds)*time.Second)
		}
		if date, err := time.Parse(time.RFC1123, values[0]); err == nil {
			return exporterhelper.NewThrottleRetry(formattedErr, time.Until(date))
		}
	}
	return formattedErr
}

// Determine if the status code is retryable according to the specification.
// For more, see https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#failures-1
func isRetryableStatusCode(code int) bool {
	switch code {
	case http.StatusTooManyRequests:
		return true
	case http.StatusBadGateway:
		return true
	case http.StatusServiceUnavailable:
		return true
	case http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	if resp.ContentLength == 0 {
		return nil, nil
	}

	maxRead := resp.ContentLength

	// if maxRead == -1, the ContentLength header has not been sent, so read up to
	// the maximum permitted body size. If it is larger than the permitted body
	// size, still try to read from the body in case the value is an error. If the
	// body is larger than the maximum size, proto unmarshaling will likely fail.
	if maxRead == -1 || maxRead > maxHTTPResponseReadBytes {
		maxRead = maxHTTPResponseReadBytes
	}
	protoBytes := make([]byte, maxRead)
	n, err := io.ReadFull(resp.Body, protoBytes)

	// No bytes read and an EOF error indicates there is no body to read.
	if n == 0 && (err == nil || errors.Is(err, io.EOF)) {
		return nil, nil
	}

	// io.ReadFull will return io.ErrorUnexpectedEOF if the Content-Length header
	// wasn't set, since we will try to read past the length of the body. If this
	// is the case, the body will still have the full message in it, so we want to
	// ignore the error and parse the message.
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}

	return protoBytes[:n], nil
}

// Read the response and decode the status.Status from the body.
// Returns nil if the response is empty or cannot be decoded.
func readResponseStatus(resp *http.Response) *status.Status {
	var respStatus *status.Status
	if resp.StatusCode >= 400 && resp.StatusCode <= 599 {
		// Request failed. Read the body. OTLP spec says:
		// "Response body for all HTTP 4xx and HTTP 5xx responses MUST be a
		// Protobuf-encoded Status message that describes the problem."
		respBytes, err := readResponseBody(resp)
		if err != nil {
			return nil
		}

		// Decode it as Status struct. See https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#failures
		respStatus = &status.Status{}
		err = proto.Unmarshal(respBytes, respStatus)
		if err != nil {
			return nil
		}
	}

	return respStatus
}

func handlePartialSuccessResponse(resp *http.Response, partialSuccessHandler partialSuccessHandler) error {
	bodyBytes, err := readResponseBody(resp)
	if err != nil {
		return err
	}

	return partialSuccessHandler(bodyBytes, resp.Header.Get("Content-Type"))
}

type partialSuccessHandler func(bytes []byte, contentType string) error

func (e *baseExporter) tracesPartialSuccessHandler(protoBytes []byte, contentType string) error {
	if protoBytes == nil {
		return nil
	}
	exportResponse := ptraceotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if partialSuccess.ErrorMessage() != "" || partialSuccess.RejectedSpans() != 0 {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_spans", exportResponse.PartialSuccess().RejectedSpans()),
		)
	}
	return nil
}

func (e *baseExporter) metricsPartialSuccessHandler(protoBytes []byte, contentType string) error {
	if protoBytes == nil {
		return nil
	}
	exportResponse := pmetricotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if partialSuccess.ErrorMessage() != "" || partialSuccess.RejectedDataPoints() != 0 {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_data_points", exportResponse.PartialSuccess().RejectedDataPoints()),
		)
	}
	return nil
}

func (e *baseExporter) logsPartialSuccessHandler(protoBytes []byte, contentType string) error {
	if protoBytes == nil {
		return nil
	}
	exportResponse := plogotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if partialSuccess.ErrorMessage() != "" || partialSuccess.RejectedLogRecords() != 0 {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_log_records", exportResponse.PartialSuccess().RejectedLogRecords()),
		)
	}
	return nil
}

func (e *baseExporter) profilesPartialSuccessHandler(protoBytes []byte, contentType string) error {
	if protoBytes == nil {
		return nil
	}
	exportResponse := pprofileotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if partialSuccess.ErrorMessage() != "" || partialSuccess.RejectedProfiles() != 0 {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_samples", exportResponse.PartialSuccess().RejectedProfiles()),
		)
	}
	return nil
}
