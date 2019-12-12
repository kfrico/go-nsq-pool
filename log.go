package nsqpool

// logger interface
type logger interface {
	Output(calldepth int, s string) error
}

// LogLevel specifies the severity of a given log message
type LogLevel int

// Log levels
const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
)
