package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/touchtechnologies-product/message-broker/common"
)

type contextKey string

const contextKeyValue = "key"

type consumer struct {
	ready    chan bool
	handlers map[string]common.Handler
	topics   []string
}

func (c *consumer) Setup(sarama.ConsumerGroupSession) (err error) {
	c.becomeReady()
	return nil
}

func (c *consumer) becomeReady() {
	defer func() {recover()}()
	close(c.ready)
}

func (c *consumer) Cleanup(sarama.ConsumerGroupSession) (err error) {
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	for msg := range claim.Messages() {
		c.handleMessage(msg)
		session.MarkMessage(msg, "")
	}

	return nil
}

func (c *consumer) handleMessage(msg *sarama.ConsumerMessage) {
	ck := contextKey(contextKeyValue)
	ctx := context.WithValue(context.Background(), ck, msg.Key)
	c.handlers[msg.Topic](ctx, msg.Value)
}

func (kafka *broker) initGroupHandler() {
	kafka.consumer = &consumer{
		ready:    make(chan bool),
		handlers: map[string]common.Handler{},
		topics:   []string{},
	}
}
