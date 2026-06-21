package broker

import (
	"fmt"
	"sync"
	"testing"
)

// TestPartitionConcurrentAppend hammers a single partition with many
// concurrent producers and checks that every offset 0..N-1 was
// assigned exactly once with no duplicates or gaps. Run with -race.
func TestPartitionConcurrentAppend(t *testing.T) {
	p := NewPartition("test", 0)
	defer p.Close()

	const producers = 50
	const perProducer = 100
	total := producers * perProducer

	var wg sync.WaitGroup
	offsets := make(chan int64, total)

	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				off, err := p.Append([]byte(fmt.Sprintf("k%d", id)), []byte("v"))
				if err != nil {
					t.Errorf("append error: %v", err)
					return
				}
				offsets <- off
			}
		}(i)
	}
	wg.Wait()
	close(offsets)

	seen := make(map[int64]bool, total)
	for off := range offsets {
		if seen[off] {
			t.Fatalf("duplicate offset assigned: %d", off)
		}
		seen[off] = true
	}
	if len(seen) != total {
		t.Fatalf("expected %d unique offsets, got %d", total, len(seen))
	}
	if got := p.LatestOffset(); got != int64(total) {
		t.Fatalf("expected LatestOffset=%d, got %d", total, got)
	}
}

func TestTopicKeyOrdering(t *testing.T) {
	topic := NewTopic("orders", 4)
	defer topic.Close()

	key := []byte("same-key")
	var lastPartition int32 = -1
	for i := 0; i < 10; i++ {
		pid, _, err := topic.Produce(key, []byte("v"))
		if err != nil {
			t.Fatalf("produce error: %v", err)
		}
		if lastPartition != -1 && pid != lastPartition {
			t.Fatalf("same key routed to different partitions: %d vs %d", lastPartition, pid)
		}
		lastPartition = pid
	}
}
