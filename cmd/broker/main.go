package main

import (
	"fmt"
	"sync"

	"github.com/parkheejha10/minikafka/broker"
)

func main() {
	topic := broker.NewTopic("orders", 3)
	defer topic.Close()

	var wg sync.WaitGroup
	// Simulate 5 concurrent producers, 20 messages each.
	for p := 0; p < 5; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				key := []byte(fmt.Sprintf("user-%d", producerID))
				value := []byte(fmt.Sprintf("event-%d-%d", producerID, i))
				partitionID, offset, err := topic.Produce(key, value)
				if err != nil {
					fmt.Println("produce error:", err)
					return
				}
				_ = partitionID
				_ = offset
			}
		}(p)
	}
	wg.Wait()

	fmt.Println("Produced 100 messages across 3 partitions. Per-partition counts:")
	for i := int32(0); i < topic.NumPartitions(); i++ {
		fmt.Printf("  partition %d: %d messages\n", i, topic.Partition(i).LatestOffset())
	}

	fmt.Println("\nFetching first 5 messages from partition 0:")
	msgs := topic.Partition(0).Fetch(0, 5)
	for _, m := range msgs {
		fmt.Printf("  offset=%d key=%s value=%s\n", m.Offset, m.Key, m.Value)
	}
}
