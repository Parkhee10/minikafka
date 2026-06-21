package broker

import (
	"errors"
	"sync"
	"time"
)

var ErrPartitionClosed = errors.New("partition is closed")

type appendRequest struct {
	msg      Message
	resultCh chan appendResult
}

type appendResult struct {
	offset int64
	err    error
}

// Partition owns a single append-only log, now backed by a WAL on
// disk in addition to the in-memory copy. All writes are serialized
// through one goroutine so there is never lock contention on the
// write path and offset assignment is trivially correct.
type Partition struct {
	id    int32
	topic string

	mu  sync.RWMutex
	log []Message

	wal *WAL // nil if running without persistence (e.g. in tests)

	appendCh chan appendRequest
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

// NewPartition creates a partition with no persistence — useful for
// tests and the Day 1 in-memory demo. Use NewPersistentPartition for
// real usage with disk-backed durability.
func NewPartition(topic string, id int32) *Partition {
	p := &Partition{
		id:       id,
		topic:    topic,
		log:      make([]Message, 0, 1024),
		appendCh: make(chan appendRequest),
		closeCh:  make(chan struct{}),
	}
	p.wg.Add(1)
	go p.writerLoop()
	return p
}

// NewPersistentPartition opens (or creates) a WAL file at walPath,
// replays any existing records into memory, and then starts
// accepting new appends. This is what makes a partition survive a
// process restart: on startup, everything written before the crash
// is read back from disk before any new message can be appended.
func NewPersistentPartition(topic string, id int32, walPath string) (*Partition, error) {
	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, err
	}

	p := &Partition{
		id:       id,
		topic:    topic,
		log:      make([]Message, 0, 1024),
		wal:      wal,
		appendCh: make(chan appendRequest),
		closeCh:  make(chan struct{}),
	}

	// Rebuild in-memory state from disk BEFORE accepting new writes.
	// If this fails, we don't start the writer loop at all — better
	// to fail fast on startup than to silently run with incomplete
	// data.
	err = wal.Replay(func(msg Message) error {
		p.log = append(p.log, msg)
		return nil
	})
	if err != nil {
		return nil, err
	}

	p.wg.Add(1)
	go p.writerLoop()
	return p, nil
}

func (p *Partition) writerLoop() {
	defer p.wg.Done()
	for {
		select {
		case req := <-p.appendCh:
			p.mu.Lock()
			offset := int64(len(p.log))
			req.msg.Offset = offset
			req.msg.Timestamp = time.Now()

			// Write to disk FIRST. Only after the WAL write
			// succeeds do we consider the message durable and
			// add it to the in-memory log — this ordering is the
			// entire point of "write-ahead": disk before memory,
			// so a crash between the two never loses data that
			// was reported as successfully appended.
			var walErr error
			if p.wal != nil {
				walErr = p.wal.Append(req.msg)
			}
			if walErr == nil {
				p.log = append(p.log, req.msg)
			}
			p.mu.Unlock()

			req.resultCh <- appendResult{offset: offset, err: walErr}
		case <-p.closeCh:
			return
		}
	}
}

func (p *Partition) Append(key, value []byte) (int64, error) {
	resultCh := make(chan appendResult, 1)
	req := appendRequest{
		msg:      Message{Key: key, Value: value},
		resultCh: resultCh,
	}

	select {
	case p.appendCh <- req:
	case <-p.closeCh:
		return 0, ErrPartitionClosed
	}

	res := <-resultCh
	return res.offset, res.err
}

func (p *Partition) Fetch(fromOffset int64, maxMessages int) []Message {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if fromOffset < 0 || fromOffset >= int64(len(p.log)) {
		return nil
	}
	end := fromOffset + int64(maxMessages)
	if end > int64(len(p.log)) {
		end = int64(len(p.log))
	}
	out := make([]Message, end-fromOffset)
	copy(out, p.log[fromOffset:end])
	return out
}

func (p *Partition) LatestOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int64(len(p.log))
}

func (p *Partition) Close() {
	close(p.closeCh)
	p.wg.Wait()
	if p.wal != nil {
		p.wal.Close()
	}
}
