package kafka

import (
	"github.com/Shopify/sarama"
)

func (kafka *broker) initConsumerGroup(conf *sarama.Config) (err error) {
	conf.ClientID = kafka.uuid.Generate().String()
	kafka.consumerGroup, err = sarama.NewConsumerGroup(kafka.conf.Host, kafka.conf.Group, conf)
	return err
}