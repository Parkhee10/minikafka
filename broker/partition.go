package broker

import (
	"errors"
	"sync"
	"time"
)

var ErrPartitionClosed = errors.New("partition is closed")

// appendRequest is sent to the partition's writer goroutine.
// resultCh receives the assigned offset (or an error) once the
// append has been durably applied.
type appendRequest struct {
	msg      Message
	resultCh chan appendResult
}

type appendResult struct {
	offset int64
	err    error
}

// Partition owns a single append-only log. All writes are serialized
// through one goroutine (run via appendCh) so there is never lock
// contention on the write path, and writes naturally happen in the
// order producers submitted them. Reads go straight to the in-memory
// slice under an RWMutex, since reads are far more frequent than
// writes in a typical pub/sub workload and shouldn't have to wait
// behind the writer goroutine.
type Partition struct {
	id    int32
	topic string

	mu  sync.RWMutex // protects log
	log []Message    // in-memory log; Day 3 adds WAL-backed persistence

	appendCh chan appendRequest
	closeCh  chan struct{}
	wg       sync.WaitGroup
}

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

// writerLoop is the single goroutine permitted to mutate the log.
// Centralizing writes here means offset assignment is trivially
// correct (no race between "read last offset" and "append") without
// needing a lock on the hot write path.
func (p *Partition) writerLoop() {
	defer p.wg.Done()
	for {
		select {
		case req := <-p.appendCh:
			p.mu.Lock()
			offset := int64(len(p.log))
			req.msg.Offset = offset
			req.msg.Timestamp = time.Now()
			p.log = append(p.log, req.msg)
			p.mu.Unlock()
			req.resultCh <- appendResult{offset: offset}
		case <-p.closeCh:
			return
		}
	}
}

// Append submits a message to be written and blocks until it has
// been assigned an offset. Safe to call concurrently from many
// goroutines (e.g. many gRPC handler goroutines for many producers).
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

// Fetch returns up to maxMessages starting at fromOffset.
// Read-only, so it takes the RLock and never touches appendCh.
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
	// Copy out rather than returning a sub-slice of the live log,
	// so callers can't accidentally retain a reference that aliases
	// memory the writer goroutine may later resize via append().
	out := make([]Message, end-fromOffset)
	copy(out, p.log[fromOffset:end])
	return out
}

// LatestOffset returns the offset that would be assigned to the
// next appended message (i.e. current log length).
func (p *Partition) LatestOffset() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return int64(len(p.log))
}

func (p *Partition) Close() {
	close(p.closeCh)
	p.wg.Wait()
}
