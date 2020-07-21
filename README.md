Current supported broker
----------
1. Kafka

To add new supported broker
----------
1. Implement a "Broker" interface
2. Add a new case to switch in "NewBroker" in "broker.go" file

Configuration options
----------
|Option          |Type     |Description                                |
|----------------|---------|-------------------------------------------|
|BackOffTime     |int      |A delay in seconds between each retry      |
|MaximumRetry    |int      |A maximum number of retry                  |
|Version         |string   |A version of Kafka                         |
|Group           |string   |A name of a consumer group                 |
|Host            |[]string |A list of Kafka hosts, eg. "localhost:9092"|
|Debug           |bool     |Set to true to display Kafka log message   |

Usage example
----------
```
conf := *common.Config{
    BackOffTime:  2,
    MaximumRetry: 3,
    Version:      "2.5.1",
    Group:        "my-group",
    Host:         []string{"localhost:9092"},
    Debug:        true,
}

broker, err = NewBroker(common.KafkaBrokerType, conf)
if err != nil {
    panic(err)
}

handler := func(ctx context.Context, msg []byte) {
    // Do something
}
broker.RegisterHandler("my-topic", handler)

go broker.Start(func(ctx context.Context, err error) {
    // Do something after broker is cleaned-up
})

msg := []byte("my message")
err = broker.SendTopicMessage("my-topic", msg)
```