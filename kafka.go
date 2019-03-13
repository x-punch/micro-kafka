// Package kafka provides a kafka broker using sarama cluster
package kafka

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	log "github.com/micro/go-log"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/codec/json"
	sc "gopkg.in/bsm/sarama-cluster.v2"
)

type kBroker struct {
	addrs []string

	c  sarama.Client
	p  sarama.SyncProducer
	sc []*sc.Client

	scMutex sync.Mutex
	opts    broker.Options
}

type subscriber struct {
	s    *sc.Consumer
	t    string
	opts broker.SubscribeOptions
}

type publication struct {
	t  string
	c  *sc.Consumer
	km *sarama.ConsumerMessage
	m  *broker.Message
}

func init() {
	cmd.DefaultBrokers["kafka"] = NewBroker
}

func (p *publication) Topic() string {
	return p.t
}

func (p *publication) Message() *broker.Message {
	return p.m
}

func (p *publication) Ack() error {
	p.c.MarkOffset(p.km, "")
	return nil
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.t
}

func (s *subscriber) Unsubscribe() error {
	return s.s.Close()
}

func (k *kBroker) Address() string {
	if len(k.addrs) > 0 {
		return k.addrs[0]
	}
	return "127.0.0.1:9092"
}

func (k *kBroker) Connect() error {
	if k.c != nil {
		return nil
	}

	pconfig := k.getBrokerConfig()
	// For implementation reasons, the SyncProducer requires
	// `Producer.Return.Errors` and `Producer.Return.Successes`
	// to be set to true in its configuration.
	pconfig.Producer.Return.Successes = true
	pconfig.Producer.Return.Errors = true

	c, err := sarama.NewClient(k.addrs, pconfig)
	if err != nil {
		return err
	}

	k.c = c

	p, err := sarama.NewSyncProducerFromClient(c)
	if err != nil {
		return err
	}

	k.p = p
	k.scMutex.Lock()
	defer k.scMutex.Unlock()
	k.sc = make([]*sc.Client, 0)

	return nil
}

func (k *kBroker) Disconnect() error {
	k.scMutex.Lock()
	defer k.scMutex.Unlock()
	for _, client := range k.sc {
		client.Close()
	}
	k.sc = nil
	k.p.Close()
	return k.c.Close()
}

func (k *kBroker) Init(opts ...broker.Option) error {
	for _, o := range opts {
		o(&k.opts)
	}
	var cAddrs []string
	for _, addr := range k.opts.Addrs {
		if len(addr) == 0 {
			continue
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{"127.0.0.1:9092"}
	}
	k.addrs = cAddrs
	return nil
}

func (k *kBroker) Options() broker.Options {
	return k.opts
}

func (k *kBroker) Publish(topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	b, err := k.opts.Codec.Marshal(msg)
	if err != nil {
		return err
	}
	_, _, err = k.p.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(b),
	})
	return err
}

func (k *kBroker) getSaramaClusterClient(topic string) (*sc.Client, error) {
	config := k.getClusterConfig()

	cs, err := sc.NewClient(k.addrs, config)
	if err != nil {
		return nil, err
	}

	k.scMutex.Lock()
	defer k.scMutex.Unlock()
	k.sc = append(k.sc, cs)
	return cs, nil
}

func (k *kBroker) Subscribe(topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	opt := broker.SubscribeOptions{
		AutoAck: true,
		Queue:   uuid.New().String(),
	}

	for _, o := range opts {
		o(&opt)
	}

	// we need to create a new client per consumer
	cs, err := k.getSaramaClusterClient(topic)
	if err != nil {
		return nil, err
	}

	c, err := sc.NewConsumerFromClient(cs, opt.Queue, []string{topic})
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case err := <-c.Errors():
				log.Log("[kafka]Error: %+v", err)
			case ntf := <-c.Notifications():
				log.log("[kafka]%+v", ntf)
			case sm := <-c.Messages():
				// ensure message is not nil
				if sm == nil {
					continue
				}
				var m broker.Message
				if err := k.opts.Codec.Unmarshal(sm.Value, &m); err != nil {
					continue
				}
				if err := handler(&publication{
					m:  &m,
					t:  sm.Topic,
					c:  c,
					km: sm,
				}); err == nil && opt.AutoAck {
					c.MarkOffset(sm, "")
				}
			}
		}
	}()

	return &subscriber{s: c, opts: opt}, nil
}

func (k *kBroker) String() string {
	return "kafka"
}

func NewBroker(opts ...broker.Option) broker.Broker {
	options := broker.Options{
		// default to json codec
		Codec:   json.Marshaler{},
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}

	var cAddrs []string
	for _, addr := range options.Addrs {
		if len(addr) == 0 {
			continue
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{"127.0.0.1:9092"}
	}

	return &kBroker{
		addrs: cAddrs,
		opts:  options,
	}
}

func (k *kBroker) getBrokerConfig() *sarama.Config {
	if c, ok := k.opts.Context.Value(brokerConfigKey{}).(*sarama.Config); ok {
		return c
	}
	return DefaultBrokerConfig
}

func (k *kBroker) getClusterConfig() *sc.Config {
	if c, ok := k.opts.Context.Value(clusterConfigKey{}).(*sc.Config); ok {
		return c
	}
	clusterConfig := DefaultClusterConfig
	clusterConfig.Config.Consumer.Offsets.Initial = sarama.OffsetNewest
	return clusterConfig
}
