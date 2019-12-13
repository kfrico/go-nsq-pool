package nsqpool

import (
	"time"

	"github.com/nsqio/go-nsq"
)

// NsqProducer interface
type NsqProducer interface {
	Ping() error
	SetLogger(l logger, lvl LogLevel)
	String() string
	Stop()
	PublishAsync(topic string, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	MultiPublishAsync(topic string, body [][]byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
	Publish(topic string, body []byte) error
	MultiPublish(topic string, body [][]byte) error
	DeferredPublish(topic string, delay time.Duration, body []byte) error
	DeferredPublishAsync(topic string, delay time.Duration, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error
}

// NewProducer returns an instance of Producer for the specified address
func NewProducer(addr string, config *nsq.Config, num int) (NsqProducer, error) {
	var err error

	p := make(map[int]*nsq.Producer, num)

	for i := 0; i < num; i++ {
		p[i], err = nsq.NewProducer(addr, config)

		if err != nil {
			return nil, err
		}
	}

	pp := &Producer{
		pool:     p,
		num:      num,
		addr:     addr,
		config:   *config,
		worker:   make(chan *nsq.Producer, num),
		stopChan: make(chan struct{}),
	}

	go pp.work()

	return pp, nil
}

// Producer type
type Producer struct {
	pool     map[int]*nsq.Producer
	num      int
	addr     string
	config   nsq.Config
	next     int
	worker   chan *nsq.Producer
	stopChan chan struct{}
}

// work job work
func (pp *Producer) work() {
	for {
		pp.next = (pp.next + 1) % pp.num
		select {
		case pp.worker <- pp.pool[pp.next]:
		case <-pp.stopChan:
			return
		}
	}
}

// Each each pool producer
func (pp *Producer) Each(callback func(*nsq.Producer) error) error {
	for _, p := range pp.pool {
		err := callback(p)

		if err != nil {
			return err
		}
	}

	return nil
}

// Next get next producer
func (pp *Producer) Next() *nsq.Producer {
	return <-pp.worker
}

// Ping causes the Producer to connect to it's configured nsqd (if not already
// connected) and send a `Nop` command, returning any error that might occur.
func (pp *Producer) Ping() error {
	err := pp.Each(func(p *nsq.Producer) error {
		return p.Ping()
	})

	return err
}

// SetLogger assigns the logger to use as well as a level
func (pp *Producer) SetLogger(l logger, lvl LogLevel) {
	_ = pp.Each(func(p *nsq.Producer) error {
		p.SetLogger(l, nsq.LogLevel(lvl))

		return nil
	})
}

// String returns the address of the Producer
func (pp *Producer) String() string {
	return pp.addr
}

// Stop initiates a graceful stop of the Producer (permanent)
func (pp *Producer) Stop() {
	_ = pp.Each(func(p *nsq.Producer) error {
		p.Stop()

		return nil
	})

	pp.stopChan <- struct{}{}
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
func (pp *Producer) PublishAsync(topic string, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return pp.Next().PublishAsync(topic, body, doneChan, args)
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
func (pp *Producer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return pp.Next().MultiPublishAsync(topic, body, doneChan, args)
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed
func (pp *Producer) Publish(topic string, body []byte) error {
	return pp.Next().Publish(topic, body)
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed
func (pp *Producer) MultiPublish(topic string, body [][]byte) error {
	return pp.Next().MultiPublish(topic, body)
}

// DeferredPublish synchronously publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires, returning
// an error if publish failed
func (pp *Producer) DeferredPublish(topic string, delay time.Duration, body []byte) error {
	return pp.Next().DeferredPublish(topic, delay, body)
}

// DeferredPublishAsync publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires
// but does not wait for the response from `nsqd`.
func (pp *Producer) DeferredPublishAsync(topic string, delay time.Duration, body []byte, doneChan chan *nsq.ProducerTransaction, args ...interface{}) error {
	return pp.Next().DeferredPublishAsync(topic, delay, body, doneChan, args)
}
