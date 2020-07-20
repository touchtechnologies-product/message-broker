package kafka

import (
	"context"
	"fmt"
	"github.com/touchtechnologies-product/message-broker/common"
	"github.com/touchtechnologies-product/retry"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bwmarrin/snowflake"
)

const MaxSnowFlakeNodeNum = 1024

type Broker struct {
	config   *common.Config
	uuid     *snowflake.Node
	consumer sarama.ConsumerGroup
	producer sarama.SyncProducer
}

func MakeKafkaBroker(conf *common.Config) (k *Broker, err error) {
	k = &Broker{}
	k.initUUIDGenerator()

	err = k.initSyncProducerAndConsumerGroup(conf)
	if err != nil {
		return nil, err
	}

	return k, k.initConsumer(conf.BackOffTime, conf.MaximumRetry)
}

func (broker *Broker) initUUIDGenerator() {
	rand.Seed(time.Now().UnixNano())
	nodeNum := rand.Intn(MaxSnowFlakeNodeNum) - 1
	broker.uuid, _ = snowflake.NewNode(int64(nodeNum))
}

func (broker *Broker) initSyncProducerAndConsumerGroup(config *common.Config) (err error) {
	conf, err := broker.newSaramaConfig(config)
	if err != nil {
		return err
	}

	conf.ClientID = broker.uuid.Generate().String()
	broker.producer, _ = sarama.NewSyncProducer(config.Host, conf)

	conf.ClientID = broker.uuid.Generate().String()
	broker.consumer, err = sarama.NewConsumerGroup(config.Host, config.Group, conf)

	return err
}

func (broker *Broker) newSaramaConfig(config *common.Config) (conf *sarama.Config, err error) {
	if config.Debug {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	saramaVersion, err := sarama.ParseKafkaVersion(config.Version)
	if err != nil {
		return nil, err
	}

	conf = sarama.NewConfig()
	conf.Version = saramaVersion
	conf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	conf.Producer.Return.Errors = true
	conf.Producer.Return.Successes = true

	return conf, nil
}

func (broker *Broker) initConsumer(backOffTime int, maxRetry int) (err error) {
	broker.consumer = &consumer{
		ready:    make(chan bool),
		handlers: map[string]common.Handler{},
		topics:   []string{},
	}

	broker.consumer.retryManager, err = retry.NewManager(retry.InMemType, backOffTime, maxRetry)

	return err
}

func (broker *Broker) RegisterHandler(topic string, handler common.Handler) {
	broker.consumer.topics = append(broker.consumer.topics, topic)
	broker.consumer.handlers[topic] = handler
}

func (broker *Broker) Start() {
	go broker.startConsumingMsg()
	broker.waitForStopSignal()
}

func (broker *Broker) startConsumingMsg() {
	go func() {
		for {
			_ = broker.consumerGroup.Consume(context.Background(), broker.consumer.topics, broker.consumer)
			broker.consumer.ready = make(chan bool)
		}
	}()

	<-broker.consumer.ready
}

func (broker *Broker) waitForStopSignal() {
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-broker.ctx.Done():
		fmt.Println("terminating: context cancelled")
	}

	broker.Cleanup()
}

func (broker *Broker) Stop() {
	broker.cancel()
}

func (broker *Broker) Cleanup() {
	_ = broker.consumer.Cleanup(nil)
	_ = broker.producer.Close()
	_ = broker.consumerGroup.Close()
}

func (broker *Broker) SendMessage(topic string, msg []byte) (err error) {
	uuid := broker.uuid.Generate().String()

	saramaMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(uuid),
		Value: sarama.StringEncoder(msg),
	}

	_, _, err = broker.producer.SendMessage(saramaMsg)

	return err
}
