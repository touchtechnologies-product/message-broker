package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/touchtechnologies-product/message-broker/common"
)

type contextKey string

const contextKeyValue = "key"

type groupHandler struct {
	handlers map[string]common.Handler
	topics   []string
}

func (group *groupHandler) Setup(sarama.ConsumerGroupSession) (err error) {
	return nil
}

func (group *groupHandler) Cleanup(sarama.ConsumerGroupSession) (err error) {
	return nil
}

func (group *groupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	for msg := range claim.Messages() {
		err = group.handleMessage(msg)
		session.MarkMessage(msg, "")
		if err != nil {
			break
		}
	}

	return err
}

func (group *groupHandler) handleMessage(msg *sarama.ConsumerMessage) (err error) {
	ck := contextKey(contextKeyValue)
	ctx := context.WithValue(context.Background(), ck, msg.Key)
	return group.handlers[msg.Topic](ctx, msg.Value)
}

func (kafka *broker) initGroupHandler() {
	kafka.groupHandler = &groupHandler{
		handlers: map[string]common.Handler{},
		topics: []string{},
	}
}
