package broker

import "sync"

// ConsumerGroup tracks, for a single named group, which offset has
// been committed for each (topic, partition) pair. This is how
// "resume where I left off" works: a consumer asks "what's my last
// committed offset for this partition?", reads from there, then
// commits the new offset once it's done processing.
//
// Real Kafka stores this in a special internal topic; we keep it
// simple with an in-memory map protected by a mutex, since commits
// are infrequent compared to reads/writes on the log itself.
type ConsumerGroup struct {
	name string

	mu      sync.RWMutex
	offsets map[string]int64 // key: "topic:partition" -> committed offset
}

func NewConsumerGroup(name string) *ConsumerGroup {
	return &ConsumerGroup{
		name:    name,
		offsets: make(map[string]int64),
	}
}

func offsetKey(topic string, partition int32) string {
	return topic + ":" + itoa(partition)
}

// itoa avoids importing strconv just for this one conversion.
func itoa(n int32) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// CommittedOffset returns the last committed offset for this
// topic/partition, or 0 if the group has never committed for it
// (meaning: start from the very beginning of the log).
func (cg *ConsumerGroup) CommittedOffset(topic string, partition int32) int64 {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return cg.offsets[offsetKey(topic, partition)]
}

// Commit records that this group has successfully processed all
// messages up to and including offset, for this topic/partition.
// The next Fetch should start at offset+1.
func (cg *ConsumerGroup) Commit(topic string, partition int32, offset int64) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.offsets[offsetKey(topic, partition)] = offset
}
