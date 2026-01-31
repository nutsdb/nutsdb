package utils

import "log"

var (
	printLoggerInstance ILogger = defaultLogger()
)

type ILogger interface {
	// Printf formats according to a format specifier and writes to the logger.
	// Arguments are handled in the manner of fmt.Printf.
	Printf(string, ...any)
}

type defaultPrintLogger struct {
	l *log.Logger
}

func (dpl *defaultPrintLogger) Printf(fmt string, args ...any) {
	dpl.l.Printf(fmt, args...)
}

func defaultLogger() ILogger {
	return &defaultPrintLogger{
		l: log.Default(),
	}
}

func SetLogger(logger ILogger) {
	printLoggerInstance = logger
}

func GetLogger() ILogger {
	return printLoggerInstance
}
