package consumer

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

// Sarama configuration options
var (
	brokers  = "127.0.0.1:29092,127.0.0.1:39092"
	version  = "2.1.0"
	group    = "sample-group"
	topics   = "sample"
	assignor = "range"
	oldest   = true
	verbose  = false
)

// KafkaInput is used for recieving Kafka messages and
// transforming them into HTTP payloads.
type KafkaInput struct {
	sarama.ConsumerGroup
	consumer Consumer
	messages chan *sarama.ConsumerMessage
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready    chan bool
	messages chan *sarama.ConsumerMessage
}

// NewKafkaInput creates instance of kafka consumer client.
func NewKafkaInput() *KafkaInput {
	c := sarama.NewConfig()
	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	log.Println("Starting a new Sarama consumer")
	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}
	c.Version = version

	c.Consumer.Return.Errors = true

	switch assignor {
	case "sticky":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		c.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		c.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	group, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, c)
	if err != nil {
		panic(err.Error())
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready:    make(chan bool),
		messages: make(chan *sarama.ConsumerMessage, 256),
	}

	i := &KafkaInput{
		ConsumerGroup: group,
		messages:      make(chan *sarama.ConsumerMessage, 256),
		consumer:      consumer,
	}

	go i.loop([]string{topics})
	i.messages = consumer.messages
	return i
}

// ConsumeClaim and stuff
func (i *KafkaInput) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for msg := range c.Messages() {
		s.MarkMessage(msg, "")
		i.Push(msg)
	}
	return nil
}

func (i *KafkaInput) loop(topic []string) {
	ctx := context.Background()
	for {
		if err := i.Consume(ctx, []string{topics}, i); err != nil {
			return
		}
	}
}

// Push Messages
func (i *KafkaInput) Push(m *sarama.ConsumerMessage) {
	if i.consumer.messages != nil {
		// log.Printf("MSGPUSH: %v", m.Value)
		i.consumer.messages <- m
	}
}

func (i *KafkaInput) Read(data []byte) (int, error) {

	message := <-i.messages
	log.Printf("Msg: %s_%v", string(message.Value), string(message.Key))
	// if !i.config.useJSON {
	// 	copy(data, message.Value)
	// 	return len(message.Value), nil
	// }

	// var kafkaMessage KafkaMessage
	// json.Unmarshal(message.Value, &kafkaMessage)

	// buf, err := kafkaMessage.Dump()
	// if err != nil {
	// 	log.Println("Failed to decode access log entry:", err)
	// 	return 0, err
	// }

	// copy(data, buf)

	// return len(buf), nil

	return 0, nil
}

// func (i *KafkaInput) String() string {
//     return "Kafka Input: " + i.config.host + "/" + i.config.topic
// }

// Setup is run at the beginning of a new session, before ConsumeClaim
func (i *KafkaInput) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (i *KafkaInput) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}
