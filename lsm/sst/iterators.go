package sst

import (
	"encoding/binary"
	"io"
)

func newBytesIterator(block []byte) (*BytesIterator, int, error) {
	if len(block) == 0 {
		return nil, 0, io.EOF
	}

	it := &BytesIterator{
		buf: block,
		key: nil,
		val: nil,
	}
	// key, val, n, err := it.next()
	// if err != nil {
	// 	return nil, n, err
	// }

	// it.key = key
	// it.val = val

	return it, 0, nil
}

type BytesIterator struct {
	buf []byte
	key []byte
	val []byte
	err error
	n   int
}

func (bi *BytesIterator) hasNext() bool {
	return bi.n <= len(bi.buf) && bi.err == nil
}

func (bi *BytesIterator) read() ([]byte, []byte, int, error) {
	var nn = 0
	if len(bi.buf) == 0 {
		return nil, nil, nn, io.EOF
	}

	kl, n := binary.Uvarint(bi.buf[bi.n:])
	bi.n += n
	nn += n

	vl, n := binary.Uvarint(bi.buf[bi.n:])
	bi.n += n
	nn += n

	key := bi.buf[bi.n : bi.n+int(kl)]
	bi.n += int(kl)
	nn += int(kl)

	val := bi.buf[bi.n : bi.n+int(vl)]
	bi.n += int(vl)
	nn += int(vl)

	return key, val, nn, nil
}

func (bi *BytesIterator) next() ([]byte, []byte, int, error) {
	nextKey, nextVal, n, err := bi.read()
	if err != nil {
		return nil, nil, n, err
	}
	bi.key = nextKey
	bi.val = nextVal

	k, v := bi.key, bi.val

	if bi.n == len(bi.buf) {
		bi.err = io.EOF

		return k, v, n, nil
	}
	//bi.n += n

	return k, v, n, nil
}

func (bi *BytesIterator) close() {
	bi.buf = nil
	bi.key = nil
	bi.val = nil
	bi.n = 0
}

func NewReaderIterator(r *Reader) (*FileIterator, error) {
	var (
		maxKeys = int(r.lenKeys) - 1
		sp      = 0       // start point at r.offsets
		ep      = maxKeys // end point at r.offsets
	)
	var (
		so  int64 = 0
		eo  int64 = r.endDataBlock
		err error
	)
	if ep != 0 {
		ep = sp + 1
		eo, err = r.readOffsetAtDataBlock(ep)
		if err != nil {
			return nil, err
		}
	}
	so, err = r.readOffsetAtDataBlock(sp)
	if err != nil {
		return nil, err
	}

	segment, err := r.readDataBlock(so, eo)
	if err != nil {
		return nil, err
	}

	it, n, err := newBytesIterator(segment)
	if err != nil {
		return nil, err
	}
	return &FileIterator{
		it:         it,
		rd:         r,
		err:        err,
		segment:    ep,
		maxsegment: int(r.lenKeys) - 1,
		end:        int(r.endDataBlock),
		n:          n,
	}, nil
}

type FileIterator struct {
	rd         *Reader
	it         *BytesIterator
	key        []byte
	val        []byte
	err        error
	segment    int
	maxsegment int
	n          int
	end        int
}

func (it *FileIterator) HasNext() bool {
	return it.n <= it.end && it.err == nil
}

func (it *FileIterator) Next() ([]byte, []byte, error) {
	if !it.it.hasNext() {
		if err := it.swap(); err != nil {
			return nil, nil, err
		}
	}

	nextKey, nextVal, n, err := it.it.next()
	if err != nil {
		it.err = err

		return nil, nil, err
	}

	it.key = nextKey
	it.val = nextVal
	it.n += n

	if it.n == it.end {
		it.err = io.EOF
	}

	return it.key, it.val, nil
}

func (it *FileIterator) swap() error {
	var (
		startOffsetBlock, endOffsetBlock = int64(0), it.rd.endDataBlock
		err                              error
	)
	startOffsetBlock, err = it.rd.readOffsetAtDataBlock(it.segment)
	if err != nil {
		return err
	}

	if it.segment < it.maxsegment {
		endOffsetBlock, err = it.rd.readOffsetAtDataBlock(it.segment + 1)
		if err != nil {
			return err
		}
	}

	block, err := it.rd.readDataBlock(startOffsetBlock, endOffsetBlock)
	if err != nil {
		return err
	}
	newit, _, err := newBytesIterator(block)
	if err != nil {
		return err
	}
	it.it = newit
	it.segment += 1

	return nil
}

func newLevelIterator(levels []*SSTLevel) *LevelIterator {
	it := &LevelIterator{
		levels: levels,
	}
	it.cur = it.levels[it.nl]
	if it.cur == nil {
		return it
	}

	files := it.levels[it.nl].Files
	it.nf = len(files) - 1
	it.file = files[it.nf]

	return it
}

type LevelIterator struct {
	levels []*SSTLevel
	file   File
	cur    *SSTLevel
	nl     int
	nf     int
}

func (it *LevelIterator) hasNext() bool {
	return it.nl < len(it.levels) && it.cur != nil
}

func (it *LevelIterator) next() File {
	f := it.file

	if it.nf == 0 {
		it.nl++

		// if last lvl
		if it.nl == len(it.levels) {
			return f
		}
		it.cur = it.levels[it.nl]
		it.nf = 0
		if len(it.cur.Files) > 0 {
			it.nf = len(it.cur.Files) - 1
		}
		if it.cur.Files != nil {
			it.file = it.cur.Files[it.nf]
		}

		return f
	}
	it.file = it.cur.Files[it.nf-1]
	it.nf--

	return f
}
