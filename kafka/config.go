package kafka

import (
	"context"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/bwmarrin/snowflake"
	"log"
	"math/rand"
	"os"
	"time"
)

const MaxSnowFlakeNodeNum = 1024

func (kafka *broker) initContext() {
	kafka.ctx, kafka.cancel = context.WithCancel(context.Background())
}

func (kafka *broker) newSaramaConfig() (conf *sarama.Config, err error) {
	if kafka.conf == nil {
		return nil, errors.New("nil-configuration")
	}

	if kafka.conf.Debug {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	saramaVersion, err := sarama.ParseKafkaVersion(kafka.conf.Version)
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

func (kafka *broker) initUUIDGenerator() {
	rand.Seed(time.Now().UnixNano())
	nodeNum := rand.Intn(MaxSnowFlakeNodeNum) - 1
	kafka.uuid, _ = snowflake.NewNode(int64(nodeNum))
}