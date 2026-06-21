package main

import (
	"fmt"
	"time"

	"github.com/parkheejha10/minikafka/broker"
)

func main() {
	topic := broker.NewTopic("orders", 3)
	defer topic.Close()

	fmt.Println("Producer starting... sending 10 messages to topic 'orders'")

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("order-%d", i))
		value := []byte(fmt.Sprintf("order placed at item %d", i))

		partitionID, offset, err := topic.Produce(key, value)
		if err != nil {
			fmt.Println("error producing:", err)
			continue
		}

		fmt.Printf("sent: key=%s -> partition=%d offset=%d\n", key, partitionID, offset)
		time.Sleep(300 * time.Millisecond) // slow down so it's readable
	}

	fmt.Println("Producer done.")
}
