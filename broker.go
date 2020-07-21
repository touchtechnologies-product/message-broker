package message

import (
	"errors"
	"github.com/touchtechnologies-product/message-broker/common"
	"github.com/touchtechnologies-product/message-broker/kafka"
)

type Broker interface {
	RegisterHandler(topic string, handler common.Handler)
	Start(errCallback common.CloseCallback)
	SendTopicMessage(topic string, msg []byte) (err error)
}

func NewBroker(brokerType string, config *common.Config) (broker Broker, err error) {
	switch brokerType {
	case common.KafkaBrokerType:
		return kafka.MakeKafkaBroker(config)
	default:
		return nil, errors.New("invalid broker type")
	}
}
