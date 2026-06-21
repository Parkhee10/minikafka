package main

import (
	"fmt"
	"time"

	"github.com/parkheejha10/minikafka/broker"
)

func main() {
	topic := broker.NewTopic("orders", 3)
	defer topic.Close()

	group := broker.NewConsumerGroup("order-processors")

	fmt.Println("Producing 10 messages first, then consuming them...")

	// Produce 10 messages (same as the producer program), so this
	// single program is self-contained and demoable on its own.
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("order-%d", i))
		value := []byte(fmt.Sprintf("order placed at item %d", i))
		topic.Produce(key, value)
	}

	fmt.Println("\nNow consuming from each partition, starting from last committed offset:")

	// For each partition, ask the consumer group "where did I leave
	// off?", read forward from there, then commit the new position.
	for p := int32(0); p < topic.NumPartitions(); p++ {
		startOffset := group.CommittedOffset("orders", p)
		messages := topic.Partition(p).Fetch(startOffset, 100)

		if len(messages) == 0 {
			fmt.Printf("partition %d: nothing new to read\n", p)
			continue
		}

		fmt.Printf("partition %d: reading from offset %d\n", p, startOffset)
		for _, m := range messages {
			fmt.Printf("  consumed: offset=%d key=%s value=%s\n", m.Offset, m.Key, m.Value)
			time.Sleep(150 * time.Millisecond)
		}

		// Commit the offset right after the last message we read,
		// so a future run of this program would resume from here
		// instead of re-reading everything.
		lastOffset := messages[len(messages)-1].Offset
		group.Commit("orders", p, lastOffset+1)
		fmt.Printf("partition %d: committed offset %d\n", p, lastOffset+1)
	}

	fmt.Println("\nConsumer done.")
}
