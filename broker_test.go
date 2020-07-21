package message

import (
	"context"
	"github.com/stretchr/testify/suite"
	"github.com/touchtechnologies-product/message-broker/common"
	"syscall"
	"testing"
	"time"
)

type TestSuite struct {
	suite.Suite
	conf       *common.Config
	msgCh      chan []byte
	otherMsgCh chan []byte
}

func (suite *TestSuite) SetupTest() {
	suite.msgCh = make(chan []byte)
	suite.otherMsgCh = make(chan []byte)
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
	otherTopic := "test-other-topic"
	handler = suite.newOtherSuccessHandler()
	broker.RegisterHandler(otherTopic, handler)

	go broker.Start(func(ctx context.Context, err error) {})
	time.Sleep(10 * time.Second)

	msg := []byte("test message")
	err = broker.SendTopicMessage(topic, msg)
	suite.NoError(err)
	otherMsg := []byte("test other message")
	err = broker.SendTopicMessage(otherTopic, otherMsg)
	suite.NoError(err)

	suite.Equal(msg, <-suite.msgCh)
	suite.Equal(otherMsg, <-suite.otherMsgCh)
}

func (suite *TestSuite) newSuccessHandler() (handler common.Handler) {
	return func(ctx context.Context, msg []byte) { suite.msgCh <- msg }
}
func (suite *TestSuite) newOtherSuccessHandler() (handler common.Handler) {
	return func(ctx context.Context, msg []byte) { suite.otherMsgCh <- msg }
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
	go broker.Start(func(ctx context.Context, err error) {})
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

	ch := make(chan error)
	go broker.Start(func(ctx context.Context, err error) { ch <- err })

	suite.Error(<-ch)
}

func (suite *TestSuite) TestSignalInterruptKafka() {
	broker, err := NewBroker(common.KafkaBrokerType, suite.conf)
	suite.NoError(err)

	topic := "test-topic"
	handler := suite.newSuccessHandler()
	broker.RegisterHandler(topic, handler)

	ch := make(chan error)
	go broker.Start(func(ctx context.Context, err error) { ch <- err })
	time.Sleep(10 * time.Second)

	_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	suite.Error(<-ch)
}

func TestTestSuite(t *testing.T) {
	suite.Run(t, new(TestSuite))
}
