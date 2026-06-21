package broker

import "time"

// Message is the fundamental unit stored in a partition's log.
// Offset is assigned by the partition at append time, not by the producer.
type Message struct {
	Key       []byte
	Value     []byte
	Offset    int64
	Timestamp time.Time
}

func unixNanoToTime(nanos int64) time.Time {
	return time.Unix(0, nanos)
}
