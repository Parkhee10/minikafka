package broker

import "sync"

// GroupMembership tracks which consumers are currently active in a
// consumer group, and which partitions each one has been assigned.
// This is separate from ConsumerGroup (which tracks committed
// offsets) because membership churns far more often than offsets —
// consumers join and leave constantly, while offset history is
// durable and long-lived.
type GroupMembership struct {
	mu sync.Mutex

	activeConsumers map[string]bool
	assignments     map[string][]int32
}

func NewGroupMembership() *GroupMembership {
	return &GroupMembership{
		activeConsumers: make(map[string]bool),
		assignments:     make(map[string][]int32),
	}
}

// Join registers a consumer as active and triggers a rebalance: all
// partitions for the topic are redistributed evenly across every
// currently active consumer. Returns the assignment for THIS consumer.
func (gm *GroupMembership) Join(consumerID string, numPartitions int32) []int32 {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.activeConsumers[consumerID] = true
	gm.rebalanceLocked(numPartitions)
	return gm.assignments[consumerID]
}

// Leave removes a consumer and triggers a rebalance of the remaining
// partitions across whoever is left.
func (gm *GroupMembership) Leave(consumerID string, numPartitions int32) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	delete(gm.activeConsumers, consumerID)
	delete(gm.assignments, consumerID)
	gm.rebalanceLocked(numPartitions)
}

func (gm *GroupMembership) ActiveConsumerCount() int {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	return len(gm.activeConsumers)
}

func (gm *GroupMembership) Assignment(consumerID string) []int32 {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	return gm.assignments[consumerID]
}

// rebalanceLocked redistributes partitions round-robin across all
// currently active consumers. Caller must hold gm.mu.
//
// This is intentionally the simplest possible rebalancing strategy:
// sort consumer IDs for determinism, then deal out partitions one
// at a time like dealing cards. It doesn't try to minimize
// partition movement between rebalances (real Kafka's "sticky"
// assignor does) — a deliberate, called-out simplification.
func (gm *GroupMembership) rebalanceLocked(numPartitions int32) {
	consumerIDs := make([]string, 0, len(gm.activeConsumers))
	for id := range gm.activeConsumers {
		consumerIDs = append(consumerIDs, id)
	}

	gm.assignments = make(map[string][]int32)

	if len(consumerIDs) == 0 {
		return
	}

	sortStrings(consumerIDs)

	for partition := int32(0); partition < numPartitions; partition++ {
		owner := consumerIDs[partition%int32(len(consumerIDs))]
		gm.assignments[owner] = append(gm.assignments[owner], partition)
	}
}

// sortStrings is a tiny insertion sort to avoid importing "sort"
// for what's normally a handful of consumer IDs.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
