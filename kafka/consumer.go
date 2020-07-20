package kafka

import (
	"context"
	"github.com/touchtechnologies-product/message-broker/common"

	"github.com/Shopify/sarama"
	"github.com/touchtechnologies-product/retry"
)

type contextKey string

const contextKeyValue = "key"

type consumer struct {
	ready        chan bool
	handlers     map[string]common.Handler
	topics       []string
	retryManager retry.Manager
}

func (consumer *consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	var msg *sarama.ConsumerMessage

	for msg = range claim.Messages() {
		err = consumer.handleMessage(session, msg)
		if err != nil {
			break
		}
	}

	return err
}

func (consumer *consumer) getKeyTopicData(msg *sarama.ConsumerMessage) (key string, topic string, data []byte) {
	key = string(msg.Key)
	topic = msg.Topic
	data = msg.Value
	return key, topic, data
}

func (consumer *consumer) processRetryAndDelay(key string) {
	consumer.retryManager.AddRetryCount(key)
	if !consumer.retryManager.IsMaximumRetry(key) {
		consumer.retryManager.DelayProcessFollowBackOffTime(key)
	}
}

func (consumer *consumer) handleMessage(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) (err error) {
	key, topic, data := consumer.getKeyTopicData(msg)

	if consumer.handlerNotExistOrMaxRetry(topic, key) {
		session.MarkMessage(msg, "")
		return nil
	}

	handler := consumer.handlers[topic]
	ck := contextKey(contextKeyValue)
	ctx := context.WithValue(context.Background(), ck, key)

	err = handler(ctx, data)
	if err != nil {
		consumer.processRetryAndDelay(key)
		return err
	}

	session.MarkMessage(msg, "")
	return err
}

func (consumer *consumer) handlerNotExistOrMaxRetry(topic string, key string) (pass bool) {
	_, isExist := consumer.handlers[topic]
	return !isExist || consumer.isMaxRetry(key)
}

func (consumer *consumer) isMaxRetry(key string) (isMax bool) {
	if consumer.retryManager.IsMaximumRetry(key) {
		consumer.retryManager.ClearRetryCount(key)
		return true
	}
	return false
}