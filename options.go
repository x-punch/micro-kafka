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
		if err := h.kopts.Codec.Unmarshal(msg.Value, &m); err != nil {
			log.Errorf("[Consume]%s: %v", h.kopts.Codec.String(), err)
			continue
		}
		msgKey := fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
		m.Header["key"] = msgKey
		err := h.handler(&publication{m: &m, t: msg.Topic, km: msg, cg: h.cg, sess: sess})
		if err != nil {
			return errors.Wrapf(err, "%d", msg.Offset)
		} else if h.subopts.AutoAck {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}
