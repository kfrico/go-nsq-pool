package nsqpool

// logger interface
type logger interface {
	Output(calldepth int, s string) error
}
