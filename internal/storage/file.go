package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/hithisisyeshwanth/RaftLock/internal/raft"
)

// FileStorage keeps the whole durable state in one file: a 4-byte big-endian
// CRC32 of the payload, then the JSON payload. Each Save rewrites it atomically.
type FileStorage struct {
	path string
}

func NewFileStorage(path string) *FileStorage {
	return &FileStorage{path: path}
}

func (f *FileStorage) Save(s raft.PersistentState) error {
	payload, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("encode state: %w", err)
	}
	data := binary.BigEndian.AppendUint32(nil, crc32.ChecksumIEEE(payload))
	data = append(data, payload...)

	// Write to a temp file and rename, so a crash leaves the old file or the new one, never half of each.
	tmp := f.path + ".tmp"
	if err := writeSynced(tmp, data); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, f.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename state file: %w", err)
	}
	// ponytail: Sync is plain fsync; macOS needs F_FULLFSYNC to flush the disk cache. Upgrade if running real nodes on macOS.
	return syncDir(filepath.Dir(f.path))
}

func (f *FileStorage) Load() (raft.PersistentState, error) {
	var s raft.PersistentState
	data, err := os.ReadFile(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, fmt.Errorf("read state file: %w", err)
	}
	if len(data) < 4 {
		return s, fmt.Errorf("state file %s is truncated (%d bytes)", f.path, len(data))
	}
	sum, payload := binary.BigEndian.Uint32(data), data[4:]
	if crc32.ChecksumIEEE(payload) != sum {
		return s, fmt.Errorf("state file %s is corrupt: checksum mismatch", f.path)
	}
	if err := json.Unmarshal(payload, &s); err != nil {
		return raft.PersistentState{}, fmt.Errorf("decode state file %s: %w", f.path, err)
	}
	return s, nil
}

func writeSynced(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create temp state file: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("write temp state file: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync temp state file: %w", err)
	}
	return file.Close()
}

// syncDir makes the rename itself durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open state dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync state dir: %w", err)
	}
	return nil
}
