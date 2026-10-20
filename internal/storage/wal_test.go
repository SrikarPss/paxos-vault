package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestReplayAndTornTail(t *testing.T) {
	dir := t.TempDir()
	replay := func(json.RawMessage) error { return nil }
	w, err := Open(dir, replay)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(map[string]int{"value": 7}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	path := filepath.Join(dir, "ledger.wal")
	good, _ := os.ReadFile(path)
	// Test both a partial header and an incomplete payload after a valid frame.
	for _, tail := range [][]byte{{0, 0, 0}, {0, 0, 0, 10, 0, 0, 0, 0, '{'}} {
		if err := os.WriteFile(path, append(append([]byte{}, good...), tail...), 0600); err != nil {
			t.Fatal(err)
		}
		count := 0
		w, err = Open(dir, func(raw json.RawMessage) error {
			count++
			var got map[string]int
			if err := json.Unmarshal(raw, &got); err != nil {
				return err
			}
			if got["value"] != 7 {
				t.Fatal(got)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		w.Close()
		stat, _ := os.Stat(path)
		if count != 1 || stat.Size() != int64(len(good)) {
			t.Fatal("torn tail not repaired")
		}
	}
}

func TestCorruptionAndLock(t *testing.T) {
	dir := t.TempDir()
	replay := func(json.RawMessage) error { return nil }
	w, err := Open(dir, replay)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(dir, replay); err == nil {
		other.Close()
		t.Fatal("shared data directory accepted")
	}
	if err := w.Append(map[string]string{"x": "y"}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	path := filepath.Join(dir, "ledger.wal")
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 1
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(dir, replay); err == nil {
		other.Close()
		t.Fatal("checksum corruption silently accepted")
	}
}
