package kafka

import (
	"github.com/Shopify/sarama"
)

func (kafka *broker) wrapMsg(topic string, msg []byte) (wrapped *sarama.ProducerMessage) {
	uuid := kafka.uuid.Generate().String()
	return &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(uuid),
		Value: sarama.StringEncoder(msg),
	}
}

func (kafka *broker) initSyncProducer(conf *sarama.Config) (err error) {
	conf.ClientID = kafka.uuid.Generate().String()
	kafka.producer, err = sarama.NewSyncProducer(kafka.conf.Host, conf)
	return err
}