package kafka

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go-common/conf"
	"go-common/log"
	"go-common/net/trace"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

const (
	_family = "kafka"
)

var (
	// ErrProducer producer error.
	ErrProducer = errors.New("kafka producer nil")
	// ErrConsumer consumer error.
	ErrConsumer = errors.New("kafka consumer nil")
)

// Producer kafka.
type Producer struct {
	sarama.AsyncProducer
	sarama.SyncProducer
	c    *conf.KafkaProducer
	addr string
}

// NewProducer new kafka async producer and retry when has error.
func NewProducer(c *conf.KafkaProducer) (p *Producer) {
	var err error
	p = &Producer{
		c:    c,
		addr: fmt.Sprintf("%s", strings.Join(c.Brokers, ",")),
	}
	if !c.Sync {
		if err = p.asyncDial(); err != nil {
			go p.reAsyncDial()
		}
	} else {
		if err = p.syncDial(); err != nil {
			go p.reSyncDial()
		}
	}
	return
}

func (p *Producer) syncDial() (err error) {
	p.SyncProducer, err = sarama.NewSyncProducer(p.c.Brokers, nil)
	return
}

func (p *Producer) reSyncDial() {
	var err error
	for {
		if err = p.syncDial(); err == nil {
			log.Info("kafka retry new sync producer ok")
			return
		}
		log.Error("dial kafka producer error(%v)", err)
		time.Sleep(time.Second)
	}
}

func (p *Producer) asyncDial() (err error) {
	if p.AsyncProducer, err = sarama.NewAsyncProducer(p.c.Brokers, nil); err == nil {
		go p.errproc()
		go p.successproc()
	}
	return
}

func (p *Producer) reAsyncDial() {
	var err error
	for {
		if err = p.asyncDial(); err == nil {
			log.Info("kafka retry new async producer ok")
			return
		}
		log.Error("dial kafka producer error(%v)", err)
		time.Sleep(time.Second)
	}
}

// errproc errors when aync producer publish messages.
// NOTE: Either Errors channel or Successes channel must be read. See the doc of AsyncProducer
func (p *Producer) errproc() {
	err := p.Errors()
	for {
		e, ok := <-err
		if !ok {
			return
		}
		log.Error("kafka producer send message(%v) failed error(%v)", e.Msg, e.Err)
		if c, ok := e.Msg.Metadata.(context.Context); ok {
			if t, ok := trace.FromContext2(c); ok {
				t.Done(&e.Err)
			}
		}
	}
}

func (p *Producer) successproc() {
	suc := p.Successes()
	for {
		msg, ok := <-suc
		if !ok {
			return
		}
		if c, ok := msg.Metadata.(context.Context); ok {
			if t, ok := trace.FromContext2(c); ok {
				t.Finish()
			}
		}
	}
}

// Input send msg to kafka
// NOTE: If producer has beed created failed, the message will lose.
func (p *Producer) Input(c context.Context, msg *sarama.ProducerMessage) (err error) {
	key, _ := msg.Key.Encode()
	if !p.c.Sync {
		if p.AsyncProducer == nil {
			err = ErrProducer
		} else {
			msg.Metadata = c
			if t, ok := trace.FromContext2(c); ok {
				t = t.Fork(_family, "async_input", p.addr)
				t.Client(string(key))
			}
			p.AsyncProducer.Input() <- msg
		}
	} else {
		if p.SyncProducer == nil {
			err = ErrProducer
		} else {
			if t, ok := trace.FromContext2(c); ok {
				t = t.Fork(_family, "sync_input", p.addr)
				t.Client(string(key))
				defer t.Done(&err)
			}
			_, _, err = p.SyncProducer.SendMessage(msg)
		}
	}
	return
}

// Close close producer.
func (p *Producer) Close() (err error) {
	if !p.c.Sync {
		if p.AsyncProducer != nil {
			return p.AsyncProducer.Close()
		}
	}
	if p.SyncProducer != nil {
		return p.SyncProducer.Close()
	}
	return
}

// Consumer kafka
type Consumer struct {
	ConsumerGroup *consumergroup.ConsumerGroup
	c             *conf.KafkaConsumer
}

// NewConsumer new a consumer.
func NewConsumer(c *conf.KafkaConsumer) (kc *Consumer) {
	var err error
	kc = &Consumer{
		c: c,
	}
	if c.Monitor != nil {
		go kc.monitor()
	}
	if err = kc.dial(); err != nil {
		go kc.redial()
	}
	return
}

func (c *Consumer) monitor() {
	mux := http.NewServeMux()
	mux.HandleFunc("/job/monitor/ping", ping)
	server := &http.Server{
		Addr:         c.c.Monitor.Addrs[0],
		Handler:      mux,
		ReadTimeout:  time.Duration(c.c.Monitor.ReadTimeout),
		WriteTimeout: time.Duration(c.c.Monitor.WriteTimeout),
	}
	if err := server.ListenAndServe(); err != nil {
		log.Error("server.ListenAndServe error(%v)", err)
		panic(err)
	}
	return
}

func ping(wr http.ResponseWriter, r *http.Request) {
	return
}
func (c *Consumer) dial() (err error) {
	cfg := consumergroup.NewConfig()
	if c.c.Offset {
		cfg.Offsets.Initial = sarama.OffsetNewest
	} else {
		cfg.Offsets.Initial = sarama.OffsetOldest
	}
	cfg.Zookeeper.Chroot = c.c.Zookeeper.Root
	cfg.Zookeeper.Timeout = time.Duration(c.c.Zookeeper.Timeout)
	c.ConsumerGroup, err = consumergroup.JoinConsumerGroup(c.c.Group, c.c.Topics, c.c.Zookeeper.Addrs, cfg)
	return
}

func (c *Consumer) redial() {
	var err error
	for {
		if err = c.dial(); err == nil {
			log.Info("kafka retry new consumer ok")
			return
		}
		log.Error("dial kafka consumer error(%v)", err)
		time.Sleep(time.Second)
	}
}

// Close close consumer.
func (c *Consumer) Close() error {
	if c.ConsumerGroup != nil {
		return c.ConsumerGroup.Close()
	}
	return nil
}
