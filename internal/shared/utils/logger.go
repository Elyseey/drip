package utils

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

// InitLogger initializes the global logger for client
// verbose: if true, shows debug level logs; if false, shows error level only
func InitLogger(verbose bool) error {
	var config zap.Config

	if verbose {
		// Verbose mode: show debug and above
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		// Production mode: only show errors
		config = zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	}

	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stderr"}

	var err error
	logger, err = config.Build()
	if err != nil {
		return err
	}

	return nil
}

// InitServerLogger initializes logger for server with info level by default
func InitServerLogger(debug bool) error {
	var config zap.Config

	if debug {
		// Debug mode: show all logs
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		// Production mode: show info and above
		config = zap.NewProductionConfig()
	}

	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stderr"}

	var err error
	logger, err = config.Build()
	if err != nil {
		return err
	}

	return nil
}

// GetLogger returns the global logger instance
func GetLogger() *zap.Logger {
	if logger == nil {
		// Fallback to a basic logger if not initialized
		logger, _ = zap.NewProduction()
	}
	return logger
}

// Info logs an info message
func Info(msg string, fields ...zap.Field) {
	GetLogger().Info(msg, fields...)
}

// Debug logs a debug message
func Debug(msg string, fields ...zap.Field) {
	GetLogger().Debug(msg, fields...)
}

// Warn logs a warning message
func Warn(msg string, fields ...zap.Field) {
	GetLogger().Warn(msg, fields...)
}

// Error logs an error message
func Error(msg string, fields ...zap.Field) {
	GetLogger().Error(msg, fields...)
}

// Fatal logs a fatal message and exits
func Fatal(msg string, fields ...zap.Field) {
	GetLogger().Fatal(msg, fields...)
	os.Exit(1)
}

// Sync flushes any buffered log entries
func Sync() {
	if logger != nil {
		_ = logger.Sync()
	}
}
