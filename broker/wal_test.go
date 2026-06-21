package broker

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestPersistentPartitionSurvivesRestart simulates a crash: write
// some messages, close the partition (as if the process died),
// then open a NEW partition pointed at the same WAL file (as if
// the process restarted) and confirm every message is still there.
func TestPersistentPartitionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test-partition.wal")

	// "Before crash": open, write 5 messages, close.
	p1, err := NewPersistentPartition("orders", 0, walPath)
	if err != nil {
		t.Fatalf("opening partition: %v", err)
	}

	const numMessages = 5
	for i := 0; i < numMessages; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		offset, err := p1.Append(key, value)
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if offset != int64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}
	p1.Close()

	// "After restart": open a brand new Partition pointed at the
	// SAME file. It should replay all 5 messages from disk.
	p2, err := NewPersistentPartition("orders", 0, walPath)
	if err != nil {
		t.Fatalf("reopening partition: %v", err)
	}
	defer p2.Close()

	if got := p2.LatestOffset(); got != numMessages {
		t.Fatalf("expected %d messages recovered, got %d", numMessages, got)
	}

	recovered := p2.Fetch(0, numMessages)
	if len(recovered) != numMessages {
		t.Fatalf("expected to fetch %d messages, got %d", numMessages, len(recovered))
	}
	for i, msg := range recovered {
		wantKey := fmt.Sprintf("key-%d", i)
		wantValue := fmt.Sprintf("value-%d", i)
		if string(msg.Key) != wantKey {
			t.Errorf("message %d: expected key %q, got %q", i, wantKey, msg.Key)
		}
		if string(msg.Value) != wantValue {
			t.Errorf("message %d: expected value %q, got %q", i, wantValue, msg.Value)
		}
		if msg.Offset != int64(i) {
			t.Errorf("message %d: expected offset %d, got %d", i, i, msg.Offset)
		}
	}

	newOffset, err := p2.Append([]byte("key-after-restart"), []byte("value-after-restart"))
	if err != nil {
		t.Fatalf("append after restart failed: %v", err)
	}
	if newOffset != numMessages {
		t.Fatalf("expected next offset to be %d, got %d", numMessages, newOffset)
	}
}

// TestWALFileActuallyExistsOnDisk is a sanity check that we're
// really writing to disk and not silently no-op'ing.
func TestWALFileActuallyExistsOnDisk(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "exists-check.wal")

	p, err := NewPersistentPartition("t", 0, walPath)
	if err != nil {
		t.Fatalf("opening partition: %v", err)
	}
	p.Append([]byte("k"), []byte("v"))
	p.Close()

	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("expected WAL file to exist on disk: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("expected WAL file to have content, but it's empty")
	}
}
