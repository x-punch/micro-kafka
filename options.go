package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/micro/go-micro/v2/broker"
	log "github.com/micro/go-micro/v2/logger"
	"github.com/pkg/errors"
)

var (
	DefaultBrokerConfig  = sarama.NewConfig()
	DefaultClusterConfig = sarama.NewConfig()
)

type brokerConfigKey struct{}
type clusterConfigKey struct{}

func BrokerConfig(c *sarama.Config) broker.Option {
	return setBrokerOption(brokerConfigKey{}, c)
}

func ClusterConfig(c *sarama.Config) broker.Option {
	return setBrokerOption(clusterConfigKey{}, c)
}

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return setSubscribeOption(subscribeContextKey{}, ctx)
}

type subscribeConfigKey struct{}

func SubscribeConfig(c *sarama.Config) broker.SubscribeOption {
	return setSubscribeOption(subscribeConfigKey{}, c)
}

// consumerGroupHandler is the implementation of sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	handler broker.Handler
	subopts broker.SubscribeOptions
	kopts   broker.Options
	cg      sarama.ConsumerGroup
	sess    sarama.ConsumerGroupSession
}

func (*consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	log.Infof("[Setup] %v:%s:%d", sess.Claims(), sess.MemberID(), sess.GenerationID())
	return nil
}

func (*consumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	log.Infof("[Cleanup] %v:%s:%d", sess.Claims(), sess.MemberID(), sess.GenerationID())
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var m broker.Message
		p := &publication{m: &m, t: msg.Topic, km: msg, cg: h.cg, sess: sess}
		eh := h.kopts.ErrorHandler

		if err := h.kopts.Codec.Unmarshal(msg.Value, &m); err != nil {
			p.err = err
			p.m.Body = msg.Value
			if eh != nil {
				eh(p)
			} else {
				log.Errorf("[Consume]%s: %v", h.kopts.Codec.String(), err)
			}
			continue
		}
		m.Header["key"] = fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)

		err := h.handler(p)
		if err == nil && h.subopts.AutoAck {
			sess.MarkMessage(msg, "")
		} else if err != nil {
			p.err = err
			if eh != nil {
				eh(p)
			} else {
				return errors.Wrapf(err, "%d", msg.Offset)
			}
		}
	}
	return nil
}
