package message

import (
	"context"
	"errors"
	"github.com/stretchr/testify/suite"
	"github.com/touchtechnologies-product/message-broker/common"
	"testing"
	"time"
)

type TestSuite struct {
	suite.Suite
	conf *common.Config
	msgCh chan []byte
	errMsgCh chan []byte
}

func (suite *TestSuite) SetupTest() {
	suite.msgCh = make(chan []byte)
	suite.errMsgCh = make(chan []byte)
	suite.conf = &common.Config{
		BackOffTime:  2,
		MaximumRetry: 3,
		Version:      "2.5.1",
		Group:        "test-group",
		Host:         []string{"localhost:9092"},
		Debug:        true,
	}
}

func (suite *TestSuite) SetupWrongVersionTest() {
	suite.conf.Version = "2.5.x"
}

func (suite *TestSuite) TestConsumeKafkaMessage() {
	broker, err := NewBroker(common.KafkaBrokerType, suite.conf)
	suite.NoError(err)

	topic := "test-topic"
	handler := suite.newSuccessHandler()
	broker.RegisterHandler(topic, handler)

	errTopic := "test-topic-err"
	errHandler := suite.newHandlerWithErrorMsg("error message")
	broker.RegisterHandler(errTopic, errHandler)

	go broker.Start()

	time.Sleep(10*time.Second)

	msg := []byte("test message")
	err = broker.SendTopicMessage(topic, msg)
	suite.NoError(err)
	err = broker.SendTopicMessage(errTopic, msg)
	suite.NoError(err)

	suite.Equal(msg, <-suite.msgCh)
	suite.Equal(msg, <-suite.errMsgCh)
}

func (suite *TestSuite) newSuccessHandler() (handler common.Handler) {
	return func(ctx context.Context, msg []byte) (err error) {
		suite.msgCh <- msg
		return nil
	}
}

func (suite *TestSuite) newHandlerWithErrorMsg(errMsg string) (handler common.Handler) {
	return func(ctx context.Context, msg []byte) (err error) {
		suite.errMsgCh <- msg
		return errors.New(errMsg)
	}
}

func (suite *TestSuite) TestNewBrokerWithInvalidBroker() {
	_, err := NewBroker("invalid-type", suite.conf)
	suite.Error(err)
}

func (suite *TestSuite) TestNewBrokerWithInvalidVersion() {
	suite.SetupWrongVersionTest()
	_, err := NewBroker(common.KafkaBrokerType, suite.conf)
	suite.Error(err)
}

func (suite *TestSuite) TestStartKafkaBrokerWithoutHandler() {
	broker, err := NewBroker(common.KafkaBrokerType, suite.conf)
	suite.NoError(err)
	broker.Start()
}

func (suite *TestSuite) TestNewKafkaBrokerWithNilConfig() {
	_, err := NewBroker(common.KafkaBrokerType, nil)
	suite.Error(err)
}

func (suite *TestSuite) TestNewKafkaBrokerWithNilHost() {
	suite.conf.Host = []string{}
	_, err := NewBroker(common.KafkaBrokerType, suite.conf)
	suite.Error(err)
}

func (suite *TestSuite) TestConsumeNoHandlerKafkaMessage() {
	broker, err := NewBroker(common.KafkaBrokerType, suite.conf)
	suite.NoError(err)

	go broker.Start()

	time.Sleep(10*time.Second)

	msg := []byte("test message")
	err = broker.SendTopicMessage("test-unregistered-topic", msg)
	suite.NoError(err)

	time.Sleep(1*time.Second)
}

func (suite *TestSuite) TestCleanupKafkaBroker() {
	broker, err := NewBroker(common.KafkaBrokerType, suite.conf)
	suite.NoError(err)

	errTopic := "test-topic-err"
	errHandler := suite.newHandlerWithErrorMsg("error message")
	broker.RegisterHandler(errTopic, errHandler)

	go broker.Start()

	time.Sleep(10*time.Second)

	msg := []byte("test message")
	err = broker.SendTopicMessage(errTopic, msg)
	suite.NoError(err)

	suite.Equal(msg, <-suite.errMsgCh)
}

func TestTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}