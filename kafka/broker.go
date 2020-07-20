package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bwmarrin/snowflake"
	"github.com/touchtechnologies-product/message-broker/common"
)

type broker struct {
	conf          *common.Config
	uuid          *snowflake.Node
	producer      sarama.SyncProducer
	consumerGroup sarama.ConsumerGroup
	groupHandler  *groupHandler
}

func MakeKafkaBroker(givenConf *common.Config) (k *broker, err error) {
	k = &broker{conf: givenConf}
	k.initUUIDGenerator()
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
	kafka.groupHandler.topics = append(kafka.groupHandler.topics, topic)
	kafka.groupHandler.handlers[topic] = handler
}

func (kafka *broker) Start() {
	for {
		ctx := context.Background()
		err := kafka.consumerGroup.Consume(ctx, kafka.groupHandler.topics, kafka.groupHandler)
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func (kafka *broker) SendTopicMessage(topic string, msg []byte) (err error) {
	wrappedMsg := kafka.wrapMsg(topic, msg)
	_, _, err = kafka.producer.SendMessage(wrappedMsg)
	return err
}
