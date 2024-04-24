package sst

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type OptionWriter func(w *Writer)

func SparseKeyDistance(sparseKeyDistance int32) OptionWriter {
	return func(w *Writer) {
		w.sparseKeyDistance = sparseKeyDistance
	}
}

func NewWriter(filepath string, options ...OptionWriter) (*Writer, error) {
	file, err := NewSSTFiles(filepath)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		fd:       file,
		buff:     bufio.NewWriter(file),
		bufidx:   bytes.NewBuffer(make([]byte, 0, sizeBuf)),
		keyNum:   0,
		dataPos:  0,
		indexPos: 0,
		n:        0,
	}

	for _, opt := range options {
		opt(w)
	}

	return w, nil
}

type Writer struct {
	fd     *os.File
	bufidx *bytes.Buffer
	buff   *bufio.Writer

	reader                    *Reader
	offsets                   []uint32
	sparseKeyDistance         int32
	keyNum                    int32
	dataPos, indexPos, sprPos int
	n                         int
	distance                  int
	key                       []byte
	offset                    int

	idxB  bool
	close bool
}

func (w *Writer) Reader() (*Reader, error) {
	r, err := NewReader(w.Name())
	if err != nil {
		return nil, err
	}
	w.reader = r

	return w.reader, nil
}

func (w *Writer) Name() string {
	return w.fd.Name()
}

func (w *Writer) Write(key, val []byte) error {
	dBytes, err := Encode(w.buff, key, val)
	if err != nil {
		return fmt.Errorf("failed to write to the data file: %w", err)
	}
	if w.distance == 0 {
		w.key = key
		w.offset = w.dataPos
	}
	w.distance += len(key) + len(val) + (2 * binary.MaxVarintLen64)

	if w.distance >= int(w.sparseKeyDistance) {
		if err = w.writeSparseKey(key); err != nil {
			return fmt.Errorf("failed to write to the file: %w", err)
		}
		w.distance = 0
		w.key = nil
	}
	w.dataPos += dBytes
	w.keyNum++
	w.n += len(key) + len(val)
	return nil
}
func (w *Writer) writeSparseKey(key []byte) error {
	if w.bufidx.Available() < len(key)+sizeCellDefault {
		w.bufidx.Grow(len(key) + sizeCellDefault)
	}

	n, err := EncodeKeyOffset(w.bufidx, w.key, w.offset)
	if err != nil {
		return err
	}

	w.offsets = append(w.offsets, uint32(w.sprPos))
	w.sprPos += n

	return nil
}

func (w *Writer) AddIdxBlock(seqNum uint64) error {
	var (
		err error
		n   int
	)

	if w.key != nil {
		if err = w.writeSparseKey(w.key); err != nil {
			return err
		}
		w.key = nil
	}

	for idx := range w.offsets {
		if n, err = binaryPutUint32(w.bufidx, w.offsets[idx]); err != nil {
			return err
		}
		w.sprPos += n
	}

	if n, err = binaryPutUint64(w.bufidx, seqNum); err != nil {
		return err
	}
	w.sprPos += n
	if n, err = binaryPutUint32(w.bufidx, uint32(len(w.offsets))); err != nil {
		return err
	}
	w.sprPos += n

	if n, err = binaryPutUint32(w.bufidx, uint32(w.sprPos+sizeCellDefault)); err != nil {
		return err
	}
	w.sprPos += n

	nIdxBlock, err := w.buff.ReadFrom(w.bufidx)
	if err != nil {
		return err
	}

	w.dataPos += int(nIdxBlock)
	w.idxB = true

	return nil
}

func (w *Writer) Bytes() int {
	return w.n
}

func (w *Writer) Len() int {
	return int(w.keyNum)
}

func (w *Writer) Close() error {
	if w.close {
		return nil
	}

	if !w.idxB {
		return fmt.Errorf("not added idx block")
	}

	if err := w.buff.Flush(); err != nil {
		return fmt.Errorf("err flush at the close: %s", err)
	}
	// fsync degrades performance!!!
	// if err := w.fd.Sync(); err != nil {
	// 	return fmt.Errorf("err sync at the close: %s", err)
	// }
	if err := w.fd.Close(); err != nil {
		return fmt.Errorf("err close at the close: %s", err)
	}

	w.close = true

	return nil
}
