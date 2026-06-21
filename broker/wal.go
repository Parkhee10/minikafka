package broker

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// WAL (Write-Ahead Log) persists every appended message to disk
// before it's considered durable, and can replay those messages
// back when the process restarts. This is what lets a Partition
// survive a crash without losing data.
//
// On-disk format per record (all integers little-endian):
//
//	[8 bytes: offset][8 bytes: timestamp unix nano]
//	[4 bytes: key length][key bytes]
//	[4 bytes: value length][value bytes]
//
// We use a fixed binary layout instead of something like JSON
// because it's trivial to parse byte-by-byte during recovery, and
// it's compact — both matter once you're appending thousands of
// records per second.
type WAL struct {
	file *os.File
	w    *bufio.Writer
}

// OpenWAL opens (or creates) the log file at path for appending.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening WAL file: %w", err)
	}
	return &WAL{
		file: f,
		w:    bufio.NewWriter(f),
	}, nil
}

// Append writes one message record to disk and flushes immediately,
// so that once Append returns successfully, the message is durable
// even if the process crashes the next instant.
func (w *WAL) Append(msg Message) error {
	var header [16]byte
	binary.LittleEndian.PutUint64(header[0:8], uint64(msg.Offset))
	binary.LittleEndian.PutUint64(header[8:16], uint64(msg.Timestamp.UnixNano()))

	if _, err := w.w.Write(header[:]); err != nil {
		return fmt.Errorf("writing WAL header: %w", err)
	}

	if err := writeLengthPrefixed(w.w, msg.Key); err != nil {
		return fmt.Errorf("writing WAL key: %w", err)
	}
	if err := writeLengthPrefixed(w.w, msg.Value); err != nil {
		return fmt.Errorf("writing WAL value: %w", err)
	}

	if err := w.w.Flush(); err != nil {
		return fmt.Errorf("flushing WAL: %w", err)
	}
	// fsync forces the OS to actually write to physical disk rather
	// than leaving it in a page cache that could be lost on power
	// loss. This is what makes the durability guarantee real.
	return w.file.Sync()
}

func writeLengthPrefixed(w io.Writer, data []byte) error {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if len(data) > 0 {
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return nil
}

// Replay reads every record from the start of the WAL file and
// calls fn for each one, in the order they were written. Used on
// startup to rebuild a Partition's in-memory log from disk.
func (w *WAL) Replay(fn func(Message) error) error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seeking WAL to start: %w", err)
	}
	r := bufio.NewReader(w.file)

	for {
		var header [16]byte
		_, err := io.ReadFull(r, header[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("reading WAL header: %w", err)
		}

		offset := int64(binary.LittleEndian.Uint64(header[0:8]))
		tsNano := int64(binary.LittleEndian.Uint64(header[8:16]))

		key, err := readLengthPrefixed(r)
		if err != nil {
			return fmt.Errorf("reading WAL key: %w", err)
		}
		value, err := readLengthPrefixed(r)
		if err != nil {
			return fmt.Errorf("reading WAL value: %w", err)
		}

		msg := Message{
			Key:    key,
			Value:  value,
			Offset: offset,
		}
		msg.Timestamp = unixNanoToTime(tsNano)

		if err := fn(msg); err != nil {
			return err
		}
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seeking WAL to end: %w", err)
	}
	return nil
}

func readLengthPrefixed(r io.Reader) ([]byte, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(lenBuf[:])
	if n == 0 {
		return nil, nil
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (w *WAL) Close() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}
