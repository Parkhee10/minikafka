package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/parkheejha10/minikafka/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to broker: %v", err)
	}
	defer conn.Close()

	client := pb.NewBrokerClient(conn)
	ctx := context.Background()

	const topic = "orders"
	const group = "order-processors"

	joinResp, err := client.JoinGroup(ctx, &pb.JoinGroupRequest{
		Group:      group,
		Topic:      topic,
		ConsumerId: "consumer-1",
	})
	if err != nil {
		log.Fatalf("failed to join group: %v", err)
	}

	fmt.Printf("Joined group %q, assigned partitions: %v\n\n", group, joinResp.AssignedPartitions)

	for _, partition := range joinResp.AssignedPartitions {
		var startOffset int64 = 0

		fetchCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.Fetch(fetchCtx, &pb.FetchRequest{
			Group:       group,
			Topic:       topic,
			Partition:   partition,
			Offset:      startOffset,
			MaxMessages: 100,
		})
		cancel()

		if err != nil {
			log.Printf("error fetching partition %d: %v", partition, err)
			continue
		}

		if len(resp.Messages) == 0 {
			fmt.Printf("partition %d: nothing to read\n", partition)
			continue
		}

		fmt.Printf("partition %d: reading %d messages\n", partition, len(resp.Messages))
		var lastOffset int64
		for _, m := range resp.Messages {
			fmt.Printf("  consumed: offset=%d key=%s value=%s\n", m.Offset, m.Key, m.Value)
			lastOffset = m.Offset
			time.Sleep(100 * time.Millisecond)
		}

		commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Commit(commitCtx, &pb.CommitRequest{
			Group:     group,
			Topic:     topic,
			Partition: partition,
			Offset:    lastOffset + 1,
		})
		cancel()
		if err != nil {
			log.Printf("error committing offset for partition %d: %v", partition, err)
		} else {
			fmt.Printf("partition %d: committed offset %d\n", partition, lastOffset+1)
		}
	}

	fmt.Println("\nConsumer done.")
}
