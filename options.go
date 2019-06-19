package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/micro/go-micro/broker"
	"github.com/micro/go-micro/util/log"
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

// consumerGroupHandler is the implementation of sarama.ConsumerGroupHandler
type consumerGroupHandler struct {
	handler broker.Handler
	subopts broker.SubscribeOptions
	kopts   broker.Options
	cg      sarama.ConsumerGroup
	sess    sarama.ConsumerGroupSession
}

func (*consumerGroupHandler) Setup(sess sarama.ConsumerGroupSession) error {
	log.Logf("[Setup] %v:%s:%d", sess.Claims(), sess.MemberID(), sess.GenerationID())
	return nil
}

func (*consumerGroupHandler) Cleanup(sess sarama.ConsumerGroupSession) error {
	log.Logf("[Clean] %v:%s:%d", sess.Claims(), sess.MemberID(), sess.GenerationID())
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var m broker.Message
		if err := h.kopts.Codec.Unmarshal(msg.Value, &m); err != nil {
			continue
		}
		m.Header["key"] = fmt.Sprintf("%s/%d/%d", msg.Topic, msg.Partition, msg.Offset)
		if err := h.handler(&publication{
			m:    &m,
			t:    msg.Topic,
			km:   msg,
			cg:   h.cg,
			sess: sess,
		}); err != nil {
			return fmt.Errorf("%d: %s", msg.Offset, err.Error())
		} else if h.subopts.AutoAck {
			sess.MarkMessage(msg, "")
		}
	}
	return nil
}
