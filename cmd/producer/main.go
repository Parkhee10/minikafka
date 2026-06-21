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

	fmt.Println("Producer starting... sending 10 messages to topic 'orders' over the network")

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("order-%d", i))
		value := []byte(fmt.Sprintf("order placed at item %d", i))

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.Produce(ctx, &pb.ProduceRequest{
			Topic: "orders",
			Key:   key,
			Value: value,
		})
		cancel()

		if err != nil {
			log.Printf("error producing message %d: %v", i, err)
			continue
		}

		fmt.Printf("sent: key=%s -> partition=%d offset=%d\n", key, resp.Partition, resp.Offset)
		time.Sleep(300 * time.Millisecond)
	}

	fmt.Println("Producer done.")
}
