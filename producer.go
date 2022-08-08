package main

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

func RunProducer() { // simple sarama producer that adds a new producer interceptor
	// Setup configuration
	config := sarama.NewConfig()
	// Return specifies what channels will be populated.
	// If they are set to true, you must read from
	// config.Producer.Return.Successes = true
	// The total number of times to retry sending a message (default 3).
	config.Producer.Retry.Max = 5
	// The level of acknowledgement reliability needed from the broker.
	config.Producer.RequiredAcks = sarama.WaitForAll
	brokers := []string{"localhost:29092"}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			// Should not reach here
			panic(err)
		}
	}()

	for {
		time.Sleep(500 * time.Millisecond)

		msg := &sarama.ProducerMessage{
			Topic: "sarama",
			// Key:   sarama.StringEncoder(strTime),
			Value: sarama.StringEncoder("Something Cool"),
		}

		select {
		case producer.Input() <- msg:
			fmt.Println("Produce message")
		case err := <-producer.Errors():
			fmt.Println("Failed to produce message:", err)
		}
	}

}
