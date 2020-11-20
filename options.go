package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/asim/nitro/v3/broker"
	log "github.com/asim/nitro/v3/logger"
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

type disableAutoAckKey struct{}

// AutoAck will automatically acknowledge messages when no error is returned
func DisableAutoAck() broker.SubscribeOption {
	return setSubscribeOption(disableAutoAckKey{}, true)
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
	erropt  broker.ErrorHandler
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
		autoAck := true
		var m broker.Message
		p := &publication{m: &m, t: msg.Topic, km: msg, cg: h.cg, sess: sess}
		eh := h.erropt

		if err := h.kopts.Codec.Unmarshal(msg.Value, &m); err != nil {
			p.err = err
			p.m.Body = msg.Value
			if eh != nil {
				eh(p.m, err)
			} else {
				log.Errorf("[Consume]%s: %v", h.kopts.Codec.String(), err)
			}
			continue
		}
		m.Header["key"] = fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)

		err := h.handler(p.m)
		ctx := h.kopts.Context
		if bval, ok := ctx.Value(disableAutoAckKey{}).(bool); ok && bval {
			autoAck = false
		}
		if err == nil && autoAck {
			sess.MarkMessage(msg, "")
		} else if err != nil {
			p.err = err
			if eh != nil {
				eh(p.m, err)
			} else {
				return errors.Wrapf(err, "%d", msg.Offset)
			}
		}
	}
	return nil
}
