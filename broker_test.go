package message

import (
	"context"
	"errors"
	"github.com/stretchr/testify/suite"
	"github.com/touchtechnologies-product/message-broker/common"
	"testing"
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

	msg := []byte("test message")
	err = broker.SendMessage(topic, msg)
	suite.NoError(err)
	err = broker.SendMessage(errTopic, msg)
	suite.NoError(err)

	suite.Equal(msg, <-suite.msgCh)
	suite.Equal(msg, <-suite.errMsgCh)

	broker.Stop()
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

func (suite *TestSuite) TestNewBrokerWithInvalidVersion() {
	suite.SetupWrongVersionTest()
	_, err := NewBroker(common.KafkaBrokerType, suite.conf)
	suite.Error(err)
}
func (suite *TestSuite) TestNewBrokerWithInvalidBroker() {
	_, err := NewBroker("invalid-type", suite.conf)
	suite.Error(err)
}

func TestTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}