package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bwmarrin/snowflake"
	"github.com/touchtechnologies-product/message-broker/common"
	"os"
	"os/signal"
	"syscall"
)

type broker struct {
	conf          *common.Config
	uuid          *snowflake.Node
	ctx           context.Context
	cancel        context.CancelFunc
	producer      sarama.SyncProducer
	consumerGroup sarama.ConsumerGroup
	consumer      *consumer
	cleanupReady  chan error
}

func MakeKafkaBroker(givenConf *common.Config) (k *broker, err error) {
	k = &broker{
		conf: givenConf,
		cleanupReady: make(chan error),
	}

	k.initUUIDGenerator()
	k.initContext()
	k.initGroupHandler()

	conf, err := k.newSaramaConfig()
	if err != nil {
		return nil, err
	}

	err = k.initSyncProducer(conf)
	if err != nil {
		return nil, err
	}

	return k, k.initConsumerGroup(conf)
}

func (kafka *broker) RegisterHandler(topic string, handler common.Handler) {
	kafka.consumer.topics = append(kafka.consumer.topics, topic)
	kafka.consumer.handlers[topic] = handler
}

func (kafka *broker) SendTopicMessage(topic string, msg []byte) (err error) {
	wrappedMsg := kafka.wrapMsg(topic, msg)
	_, _, err = kafka.producer.SendMessage(wrappedMsg)
	return err
}

func (kafka *broker) Start(closeCallback common.CloseCallback) {
	go kafka.startConsumingLoop()
	kafka.waitUntilConsumerIsReady()
	kafka.waitForCloseSignal(closeCallback)
}

func (kafka *broker) startConsumingLoop() {
	var err error
	defer func() {
		kafka.consumer.becomeReady()
		kafka.cleanupReady<-err
	}()

	for {
		err = kafka.consumerGroup.Consume(kafka.ctx, kafka.consumer.topics, kafka.consumer)
		if err != nil {
			err = fmt.Errorf("error from consumer: %v\r\n", err.Error())
			kafka.cancel()
			return
		}

		if kafka.contextIsCancelled() {
			return
		}
	}
}

func (kafka *broker) contextIsCancelled() (is bool) {
	return kafka.ctx.Err() != nil
}

func (kafka *broker) waitUntilConsumerIsReady() {
	<-kafka.consumer.ready
}
func (kafka *broker) waitUntilCleanupIsReady() (err error) {
	err = <-kafka.cleanupReady
	close(kafka.cleanupReady)
	return err
}

func (kafka *broker) waitForCloseSignal(closeCallback common.CloseCallback) {
	var err error

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-kafka.ctx.Done():
		err = errors.New("terminating: context cancelled")
	case <-sigterm:
		kafka.cancel()
		err = errors.New("terminating: signal notified")
	}

	cErr := kafka.waitUntilCleanupIsReady()
	if cErr != nil {
		err = fmt.Errorf("%s: %s", err.Error(), cErr.Error())
	}

	kafka.cleanup()

	closeCallback(kafka.ctx, err)
}

func (kafka *broker) cleanup() {
	_ = kafka.producer.Close()
	_ = kafka.consumerGroup.Close()
}
