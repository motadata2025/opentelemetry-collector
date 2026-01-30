// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package telemetry // import "go.opentelemetry.io/collector/service/telemetry"

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"go.opentelemetry.io/collector/internal/telemetry/componentattribute"
)

// newLogger creates a Logger and a LoggerProvider from Config.
func newLogger(set Settings, cfg Config) (*zap.Logger, log.LoggerProvider, error) {
	// Copied from NewProductionConfig.
	ec := zap.NewProductionEncoderConfig()
	ec.EncodeTime = zapcore.ISO8601TimeEncoder
	// Ensure logs directory exists
	currentDir, err := os.Getwd()
	if err != nil {
		return nil, nil, err
	}
	logDir := filepath.Join(currentDir, "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, nil, err
	}

	// Create the rotating file writer
	writer := NewDateRotatingWriter(logDir, "otel-collector.log")

	// Create a shared atomic level that can be updated dynamically
	atomicLevel := zap.NewAtomicLevelAt(cfg.Logs.Level)

	zapCfg := &zap.Config{
		Level:             atomicLevel,
		Development:       cfg.Logs.Development,
		Encoding:          cfg.Logs.Encoding,
		EncoderConfig:     ec,
		OutputPaths:       cfg.Logs.OutputPaths,
		ErrorOutputPaths:  cfg.Logs.ErrorOutputPaths,
		DisableCaller:     cfg.Logs.DisableCaller,
		DisableStacktrace: cfg.Logs.DisableStacktrace,
		InitialFields:     cfg.Logs.InitialFields,
	}

	if zapCfg.Encoding == "console" {
		// Human-readable timestamps for console format of logs.
		zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	}

	logger, err := zapCfg.Build(set.ZapOptions...)
	if err != nil {
		return nil, nil, err
	}

	// Add the rotating file writer to the logger
	// We use zapcore.AddSync to make the writer safe for concurrent use
	fileCore := zapcore.NewCore(
		zapcore.NewJSONEncoder(ec), // Use JSON encoder for file logs, or make this configurable
		zapcore.AddSync(writer),
		atomicLevel,
	)

	// Start watching agent.json for log level changes
	go watchAgentConfig(atomicLevel)

	// Wrap the existing core (stdout/stderr) with the file core
	logger = logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewTee(core, fileCore)
	}))

	// The attributes in set.Resource.Attributes(), which are generated in service.go, are added
	// as resource attributes for logs exported through the LoggerProvider instantiated below.
	// To make sure they are also exposed in logs written to stdout, we add them as fields to the
	// Zap core created above using WrapCore.
	// We do NOT add them to the logger using With, because that would apply to all logs, even ones
	// exported through the core that wraps the LoggerProvider, meaning that the attributes would
	// be exported twice.
	if set.Resource != nil && len(set.Resource.Attributes()) > 0 {
		logger = logger.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
			var fields []zap.Field
			for _, attr := range set.Resource.Attributes() {
				fields = append(fields, zap.String(string(attr.Key), attr.Value.Emit()))
			}

			r := zap.Dict("resource", fields...)
			return c.With([]zapcore.Field{r})
		}))
	}

	var lp log.LoggerProvider
	logger = logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		core = componentattribute.NewConsoleCoreWithAttributes(core, attribute.NewSet())

		if len(cfg.Logs.Processors) > 0 && set.SDK != nil {
			lp = set.SDK.LoggerProvider()
			core = componentattribute.NewOTelTeeCoreWithAttributes(
				core,
				lp,
				"go.opentelemetry.io/collector/service/telemetry",
				cfg.Logs.Level,
				attribute.NewSet(),
			)
		}

		if cfg.Logs.Sampling != nil && cfg.Logs.Sampling.Enabled {
			core = newSampledCore(core, cfg.Logs.Sampling)
		}

		return core
	}))

	return logger, lp, nil
}

func newSampledCore(core zapcore.Core, sc *LogsSamplingConfig) zapcore.Core {
	// Create a logger that samples every Nth message after the first M messages every S seconds
	// where N = sc.Thereafter, M = sc.Initial, S = sc.Tick.
	return componentattribute.NewSamplerCoreWithAttributes(
		core,
		sc.Tick,
		sc.Initial,
		sc.Thereafter,
	)
}

// DateRotatingWriter implements io.WriteCloser and rotates files based on date.
type DateRotatingWriter struct {
	dir          string
	baseFilename string
	currentFile  *os.File
	currentDate  string
	currentPath  string    // Track the current file path
	lastCheck    time.Time // Last time we checked if file exists
	mu           sync.Mutex
	nowFunc      func() time.Time
}

// NewDateRotatingWriter creates a new DateRotatingWriter.
func NewDateRotatingWriter(dir, baseFilename string) *DateRotatingWriter {
	return &DateRotatingWriter{
		dir:          dir,
		baseFilename: baseFilename,
		nowFunc:      time.Now,
	}
}

func (w *DateRotatingWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Check if rotation is needed
	now := w.nowFunc()
	dateStr := now.Format("02-January-2006") // Format: 28-January-2026

	// Periodic check if file was deleted (every 1 second)
	// Only check if we have an open file
	if w.currentFile != nil && time.Since(w.lastCheck) > time.Second {
		w.lastCheck = now
		if _, err := os.Stat(w.currentPath); os.IsNotExist(err) {
			// File was deleted, force close and reset so it re-opens below
			_ = w.currentFile.Close() // Best effort close
			w.currentFile = nil
		}
	}

	if w.currentFile == nil || w.currentDate != dateStr {
		if err := w.rotate(now); err != nil {
			return 0, err
		}
	}

	return w.currentFile.Write(p)
}

func (w *DateRotatingWriter) rotate(t time.Time) error {
	if w.currentFile != nil {
		if err := w.currentFile.Close(); err != nil {
			return err
		}
	}

	// Format: "28-January-2026 23-file-name"
	// Assuming "23" is the hour.
	filename := fmt.Sprintf("%s %02d-%s", t.Format("02-January-2006"), t.Hour(), w.baseFilename)
	filePath := filepath.Join(w.dir, filename)

	// Ensure the directory exists
	if err := os.MkdirAll(w.dir, 0755); err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	w.currentFile = file
	w.currentPath = filePath
	w.currentDate = t.Format("02-January-2006")
	w.lastCheck = time.Now() // Reset check time

	return nil
}

func (w *DateRotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentFile != nil {
		return w.currentFile.Close()
	}
	return nil
}

type agentConfigCtx struct {
	Agent struct {
		LogLevel     *int `json:"trace.collector.log.level"`
		LogLevelTypo *int `json:"trace.collector.log.leve"`
	} `json:"agent"`
}

func watchAgentConfig(atomicLevel zap.AtomicLevel) {
	// Locate agent.json: Always use current directory + /config/ + agent.json
	wd, err := os.Getwd()
	if err != nil {
		// Cannot get current working directory, cannot watch
		return
	}
	configPath := filepath.Join(wd, "config", "agent.json")

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	var lastLevel zapcore.Level = atomicLevel.Level()

	for range ticker.C {
		content, err := os.ReadFile(configPath)
		if err != nil {
			continue
		}

		var cfg agentConfigCtx
		if err := json.Unmarshal(content, &cfg); err != nil {
			continue
		}

		var levelVal *int
		if cfg.Agent.LogLevel != nil {
			levelVal = cfg.Agent.LogLevel
		} else if cfg.Agent.LogLevelTypo != nil {
			levelVal = cfg.Agent.LogLevelTypo
		}

		if levelVal == nil {
			continue
		}

		// Map Integer to Zap Level
		// 0 = Trace -> Map to Debug (Zap doesn't have Trace level publicly exposed easily as standard, usually Debug is lowest)
		// 1 = Debug
		// 2 = Info
		var newLevel zapcore.Level
		switch *levelVal {
		case 0, 1:
			newLevel = zapcore.DebugLevel
		case 2:
			newLevel = zapcore.InfoLevel
		default:
			// Fallback or keep current? Let's assume Info for unknown positive, or keep current.
			// Given user map 0-2, maybe >2 is warn/error?
			// User said: "value 0 = trace, 1 = debug and 2 = info."
			// Let's map strict for now.
			newLevel = zapcore.InfoLevel
		}

		if newLevel != lastLevel {
			atomicLevel.SetLevel(newLevel)
			lastLevel = newLevel
		}
	}
}
