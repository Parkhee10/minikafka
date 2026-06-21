package broker

import (
	"testing"
)

func TestSingleConsumerGetsAllPartitions(t *testing.T) {
	gm := NewGroupMembership()

	assigned := gm.Join("consumer-1", 4)
	if len(assigned) != 4 {
		t.Fatalf("expected single consumer to get all 4 partitions, got %d: %v", len(assigned), assigned)
	}
}

func TestTwoConsumersSplitPartitions(t *testing.T) {
	gm := NewGroupMembership()

	const numPartitions = 4
	gm.Join("consumer-a", numPartitions)
	a2 := gm.Join("consumer-b", numPartitions)
	a1 := gm.Assignment("consumer-a")

	if len(a1)+len(a2) != numPartitions {
		t.Fatalf("expected %d partitions total, got %d (%v) + %d (%v)",
			numPartitions, len(a1), a1, len(a2), a2)
	}

	seen := make(map[int32]bool)
	for _, p := range append(a1, a2...) {
		if seen[p] {
			t.Fatalf("partition %d assigned to more than one consumer", p)
		}
		seen[p] = true
	}
}

func TestLeaveTriggersRebalance(t *testing.T) {
	gm := NewGroupMembership()

	const numPartitions = 4
	gm.Join("consumer-a", numPartitions)
	gm.Join("consumer-b", numPartitions)

	gm.Leave("consumer-b", numPartitions)

	remaining := gm.Assignment("consumer-a")
	if len(remaining) != numPartitions {
		t.Fatalf("expected sole remaining consumer to own all %d partitions, got %d: %v",
			numPartitions, len(remaining), remaining)
	}

	if gm.ActiveConsumerCount() != 1 {
		t.Fatalf("expected 1 active consumer after leave, got %d", gm.ActiveConsumerCount())
	}
}

func TestAllConsumersLeaveLeavesNoAssignments(t *testing.T) {
	gm := NewGroupMembership()

	gm.Join("consumer-a", 3)
	gm.Leave("consumer-a", 3)

	if gm.ActiveConsumerCount() != 0 {
		t.Fatalf("expected 0 active consumers, got %d", gm.ActiveConsumerCount())
	}
	if assignment := gm.Assignment("consumer-a"); assignment != nil {
		t.Fatalf("expected no assignment for departed consumer, got %v", assignment)
	}
}
