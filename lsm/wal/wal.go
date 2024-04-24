package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/s-ilyin/lsm-distributed/lsm/memtable"
	"github.com/s-ilyin/lsm-distributed/lsm/sst"
)

const (
	// WAL имя файла.
	walDir        = "wal"
	walFileName   = "wal.db"
	indexNamePath = "wal.index.db"
)

type WAL struct {
	f      *os.File
	fIdx   *os.File
	lock   sync.RWMutex
	fsync  bool
	seqNum uint64
	root   string
}

type Option func(*WAL)

func FileSync(fsync bool) Option {
	return func(w *WAL) {
		w.fsync = fsync
	}
}

func NewWAL(dir string, options ...Option) (*WAL, error) {
	walpath := path.Join(dir, walDir)
	if _, err := os.Stat(walpath); os.IsNotExist(err) {
		if err := os.MkdirAll(walpath, os.FileMode(0777)); err != nil {
			return nil, err
		}
	}

	fIdx, err := os.OpenFile(path.Join(walpath, indexNamePath), os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path.Join(walpath, walFileName), os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		f:    f,
		fIdx: fIdx,
	}
	seq, err := readSeqNum(w.fIdx)
	if err != nil {
		return nil, err
	}

	for _, opt := range options {
		opt(w)
	}
	w.SetSequence(seq)

	w.root = walpath

	return w, nil
}

func (w *WAL) Path() string {
	return path.Join(w.root, walFileName)
}

func (w *WAL) Name() string {
	return w.f.Name()
}

func (w *WAL) Close() error {
	if err := w.f.Close(); err != nil {
		return err
	}

	if err := w.fIdx.Close(); err != nil {
		return err
	}

	return nil
}

// clearWAL closes the current file and open the new file in the truncate mode.
func (w *WAL) Clear() error {
	w.lock.Lock()
	defer w.lock.Unlock()
	walPath := path.Join(w.root, walFileName)

	if err := w.f.Close(); err != nil {
		return fmt.Errorf("failed to close the WAL file %s: %w", walPath, err)
	}

	wal, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return fmt.Errorf("failed to open the file %s: %w", walPath, err)
	}
	w.f = wal

	return nil
}

func (w *WAL) UpSequence() error {
	w.seqNum += 1
	_, err := writeSeqNum(w.seqNum, w.fIdx)
	if err != nil {
		return err
	}

	//log.Println("[debug] write seqnum", w.seqNum)

	return nil
}

func (w *WAL) SetSequence(n uint64) {
	w.seqNum = n
}

func (w *WAL) Sequence() uint64 {
	return w.seqNum
}

func writeSeqNum(seq uint64, fidx io.WriterAt) (int, error) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], seq)
	_, err := fidx.WriteAt(encoded[:], 0)
	if err != nil {
		return len(encoded), err
	}

	return len(encoded), nil
}

func readSeqNum(fidx io.ReaderAt) (uint64, error) {
	var decoded [8]byte
	_, err := fidx.ReadAt(decoded[:], 0)
	if err != nil && err != io.EOF {
		return 0, err
	}

	return binary.LittleEndian.Uint64(decoded[:]), nil
}

// Write appends entry to the WAL file.
func (w *WAL) Append(key []byte, value []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	// for safety, since the file is open in read-write mode
	// if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
	// 	return fmt.Errorf("failed to seek to the end: %w", err)
	// }

	if _, err := sst.Encode(w.f, key, value); err != nil {
		return fmt.Errorf("failed to encode and write to the file: %w", err)
	}

	if w.fsync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("failed to sync the file: %w", err)
		}
	}

	return nil
}

// loadMemTable loads MemTable from the WAL file.
func (w *WAL) LoadMem() (*memtable.Memtable, error) {
	// for safety, since the file is open in read-write mode
	// if _, err := w.f.Seek(0, io.SeekStart); err != nil {
	// 	return nil, fmt.Errorf("failed to seek to the beginning: %w", err)
	// }

	bs, err := io.ReadAll(w.f)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewReader(bs)
	mem := memtable.NewMem()
	for {
		key, value, err := sst.Decode(buf)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return mem, nil
		}

		mem.Put(key, value)
	}
}
