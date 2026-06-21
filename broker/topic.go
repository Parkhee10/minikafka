package broker

import (
	"hash/fnv"
)

// Topic owns a fixed set of partitions. Routing is by hash(key) % N,
// which guarantees that all messages with the same key land on the
// same partition and therefore preserve relative order for that key
// — the same ordering guarantee real Kafka makes.
type Topic struct {
	Name       string
	partitions []*Partition
}

func NewTopic(name string, numPartitions int32) *Topic {
	t := &Topic{
		Name:       name,
		partitions: make([]*Partition, numPartitions),
	}
	for i := int32(0); i < numPartitions; i++ {
		t.partitions[i] = NewPartition(name, i)
	}
	return t
}

func (t *Topic) NumPartitions() int32 {
	return int32(len(t.partitions))
}

func (t *Topic) Partition(id int32) *Partition {
	if id < 0 || int(id) >= len(t.partitions) {
		return nil
	}
	return t.partitions[id]
}

// partitionFor picks a partition for a given key. A nil/empty key
// defaults to partition 0 for now; round-robin for keyless messages
// can be added later if needed.
func (t *Topic) partitionFor(key []byte) int32 {
	if len(key) == 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write(key)
	return int32(h.Sum32() % uint32(len(t.partitions)))
}

// Produce routes the message to the correct partition and appends it,
// returning which partition it landed on and the offset it received.
func (t *Topic) Produce(key, value []byte) (partitionID int32, offset int64, err error) {
	partitionID = t.partitionFor(key)
	offset, err = t.partitions[partitionID].Append(key, value)
	return partitionID, offset, err
}

func (t *Topic) Close() {
	for _, p := range t.partitions {
		p.Close()
	}
}
