package nsqpool

import (
	"github.com/nsqio/go-nsq"
)

// NewConfig returns a new default nsq configuration.
func NewConfig() *nsq.Config {
	return nsq.NewConfig()
}
