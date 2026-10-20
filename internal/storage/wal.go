// Package storage provides an fsynced, checksummed append-only journal.
// The caller must serialize access. A process lock prevents shared data dirs.
package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const MaxRecord = 16 << 20

type WAL struct {
	file   *os.File
	failed error
}

func Open(dir string, replay func(json.RawMessage) error) (*WAL, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "ledger.wal"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("data directory already locked: %w", err)
	}
	fail := func(err error) (*WAL, error) { f.Close(); return nil, err }
	var offset int64
	for {
		var header [8]byte
		_, err := io.ReadFull(f, header[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			if err = f.Truncate(offset); err != nil {
				return fail(err)
			}
			break
		}
		if err != nil {
			return fail(err)
		}
		size := binary.BigEndian.Uint32(header[:4])
		if size == 0 || size > MaxRecord {
			return fail(fmt.Errorf("invalid WAL length at byte %d", offset))
		}
		payload := make([]byte, size)
		if _, err = io.ReadFull(f, payload); err == io.EOF || err == io.ErrUnexpectedEOF {
			if err = f.Truncate(offset); err != nil {
				return fail(err)
			}
			break
		} else if err != nil {
			return fail(err)
		}
		if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(header[4:]) {
			return fail(fmt.Errorf("WAL checksum mismatch at byte %d", offset))
		}
		if err = replay(payload); err != nil {
			return fail(fmt.Errorf("WAL replay at byte %d: %w", offset, err))
		}
		offset += 8 + int64(size)
	}
	if _, err = f.Seek(0, io.SeekEnd); err != nil {
		return fail(err)
	}
	if err = f.Sync(); err != nil {
		return fail(err)
	}
	// Sync the directory entry when the WAL is first created.
	d, err := os.Open(dir)
	if err != nil {
		return fail(err)
	}
	err = d.Sync()
	d.Close()
	if err != nil {
		return fail(err)
	}
	return &WAL{file: f}, nil
}

func (w *WAL) Append(v any) error {
	if w.failed != nil {
		return w.failed
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b) > MaxRecord {
		return fmt.Errorf("WAL record too large")
	}
	frame := make([]byte, 8+len(b))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(b)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(b))
	copy(frame[8:], b)
	n, err := w.file.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = w.file.Sync()
	}
	if err != nil {
		w.failed = fmt.Errorf("WAL unavailable: %w", err)
		return w.failed
	}
	return nil
}

func (w *WAL) Close() error { return w.file.Close() }
