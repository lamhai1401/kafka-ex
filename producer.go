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
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true
	config.Version = sarama.V2_1_0_0
	// The total number of times to retry sending a message (default 3).
	config.Producer.Retry.Max = 5
	// The level of acknowledgement reliability needed from the broker.
	config.Producer.RequiredAcks = sarama.WaitForAll
	brokers := []string{"127.0.0.1:29092", "127.0.0.1:39092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
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

	// msg := &sarama.ProducerMessage{
	// 	Topic:     topics,
	// 	Key:       sarama.StringEncoder(fmt.Sprintf("key %v", time.Now().UnixMilli())),
	// 	Value:     sarama.StringEncoder(fmt.Sprintf("value %v", time.Now().UnixMilli())),
	// 	Partition: 2,
	// }

	for {

		// select {
		// case producer.Input() <- getMsg():
		// 	fmt.Println("Produce message")
		// case err := <-producer.Errors():
		// 	fmt.Println("Failed to produce message:", err)
		// }

		producer.SendMessage(getMsg())
		time.Sleep(100 * time.Millisecond)
	}

}

func getMsg() *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic: topics,
		Key:   sarama.StringEncoder(fmt.Sprintf("key %v", time.Now().UnixMilli())),
		Value: sarama.StringEncoder(fmt.Sprintf("value %v", time.Now().UnixMilli())),
		// Timestamp: time.Now(),
		// Offset:    1,
		// Partition: 4,
	}
}
