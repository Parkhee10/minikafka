package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/parkheejha10/minikafka/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	numProducers := flag.Int("producers", 20, "number of concurrent producer goroutines")
	messagesEach := flag.Int("messages", 500, "messages each producer sends")
	flag.Parse()

	totalMessages := *numProducers * *messagesEach
	fmt.Printf("Load test: %d producers x %d messages each = %d total messages\n",
		*numProducers, *messagesEach, totalMessages)

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to broker: %v", err)
	}
	defer conn.Close()
	client := pb.NewBrokerClient(conn)

	var wg sync.WaitGroup
	var successCount int64
	var errorCount int64

	latencies := make([][]time.Duration, *numProducers)

	start := time.Now()

	for p := 0; p < *numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			myLatencies := make([]time.Duration, 0, *messagesEach)

			for i := 0; i < *messagesEach; i++ {
				key := []byte(fmt.Sprintf("producer-%d-key-%d", producerID, i))
				value := []byte(fmt.Sprintf("payload from producer %d message %d", producerID, i))

				reqStart := time.Now()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, err := client.Produce(ctx, &pb.ProduceRequest{
					Topic: "loadtest",
					Key:   key,
					Value: value,
				})
				cancel()
				elapsed := time.Since(reqStart)

				if err != nil {
					atomic.AddInt64(&errorCount, 1)
					continue
				}
				atomic.AddInt64(&successCount, 1)
				myLatencies = append(myLatencies, elapsed)
			}

			latencies[producerID] = myLatencies
		}(p)
	}

	wg.Wait()
	totalElapsed := time.Since(start)

	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	throughput := float64(successCount) / totalElapsed.Seconds()

	fmt.Println()
	fmt.Println("=== Results ===")
	fmt.Printf("Total time:        %v\n", totalElapsed)
	fmt.Printf("Successful sends:  %d\n", successCount)
	fmt.Printf("Failed sends:      %d\n", errorCount)
	fmt.Printf("Throughput:        %.0f messages/sec\n", throughput)
	if len(all) > 0 {
		fmt.Printf("Latency p50:       %v\n", percentile(all, 50))
		fmt.Printf("Latency p95:       %v\n", percentile(all, 95))
		fmt.Printf("Latency p99:       %v\n", percentile(all, 99))
		fmt.Printf("Latency max:       %v\n", all[len(all)-1])
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
