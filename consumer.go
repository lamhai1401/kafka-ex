package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

// Sarama configuration options
var (
	brokers = "127.0.0.1:29092,127.0.0.1:39092"
	version = "2.1.0"
	// group    = "sample"
	topics   = "sample"
	assignor = "range"
	oldest   = true
	verbose  = false
)

func RunConsumer() {
	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	case "roundrobin":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	case "range":
		config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	// consumer := Consumer{
	// 	ready: make(chan bool),
	// }

	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()
	client, err := sarama.NewConsumer(strings.Split(brokers, ","), config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	// id, err := client.Topics()
	// if err != nil {
	// 	log.Panicf("Error creating consumer group client: %v", err)
	// }
	// fmt.Println(id)

	// go func() {
	// 	for {
	// 		// `Consume` should be called inside an infinite loop, when a
	// 		// server-side rebalance happens, the consumer session will need to be
	// 		// recreated to get the new claims
	// 		if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
	// 			log.Panicf("Error from consumer: %v", err)
	// 		}
	// 		// check if context was cancelled, signaling that the consumer should stop
	// 		if ctx.Err() != nil {
	// 			return
	// 		}
	// 		consumer.ready = make(chan bool)
	// 	}
	// }()

	// How to decide partition, is it fixed value...?
	consumer, err := client.ConsumePartition(topics, 1, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	// initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic

	for {
		select {
		case err := <-consumer.Errors():
			fmt.Println(err)
		case msg := <-consumer.Messages():
			fmt.Println(msg.Offset, msg.Partition)
			fmt.Println("Received messages", string(msg.Key), string(msg.Value))
		}
	}

	// <-consumer.ready // Await till the consumer has been set up
	// fmt.Println("Sarama consumer up and running!...")

	// select {}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			fmt.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}

// https://github.com/tcnksm-sample/sarama/blob/master/http-log-producer/main.go
