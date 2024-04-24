package sst

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// errors
var (
	ErrKeyNotFound = errors.New("key not found")
)

const (
	sizeBuf         = 4 << 10
	sizeCellDefault = 1 << 2
	sizeCellMax     = 1 << 3
)

type Reader struct {
	//åbsst *bufio.Reader
	fsst *os.File
	it   *FileIterator

	buf            *bytes.Buffer
	offsets        []byte
	keysvalues     []byte
	sizeIndexBlock int64
	size           int64
	endDataBlock   int64
	seqNum         uint64

	lenKeys uint32
}

func (r *Reader) Iterator() (*FileIterator, error) {
	it, err := NewReaderIterator(r)
	if err != nil {
		return nil, err
	}
	r.it = it
	return it, nil
}

func (r *Reader) Sequence() uint64 {
	return r.seqNum
}

func (r *Reader) Close() error {
	if err := r.fsst.Close(); err != nil {
		return err
	}

	return nil
}

func (r *Reader) Name() string {
	return r.fsst.Name()
}

func NewReader(path string) (*Reader, error) {
	fsst, err := OpenBy(path)
	if err != nil {
		return nil, err
	}

	stat, err := fsst.Stat()
	if err != nil {
		return nil, err
	}

	r := &Reader{
		fsst: fsst,
		size: stat.Size(),
		buf:  bytes.NewBuffer(make([]byte, sizeBuf)),
	}

	// start read header sparse index [decode seqnum][decode len keys][decode total size idx block]
	pos := r.size - (2*sizeCellDefault + sizeCellMax)

	needed := 2*sizeCellDefault + sizeCellMax

	if r.buf.Len() < needed {
		r.buf.Grow(needed)
	}
	buf := r.buf.Bytes()[:needed]

	var n int
	if n, err = r.fsst.ReadAt(buf[:], int64(pos)); err != nil {
		return nil, err
	}
	if n < len(buf) {
		return nil, fmt.Errorf("read %d < needed %d", n, len(buf))
	}

	var (
		nn          int
		cellDefault [sizeCellDefault]byte
		cellMax     [sizeCellMax]byte
	)

	n, err = r.buf.Read(cellMax[:])
	if err != nil {
		return nil, err
	}

	r.seqNum = decodeUInt64(cellMax[:])
	nn += n

	n, err = r.buf.Read(cellDefault[:])
	if err != nil {
		return nil, err
	}

	r.lenKeys = decodeUInt32(cellDefault[:])
	nn += n

	n, err = r.buf.Read(cellDefault[:])
	if err != nil {
		return nil, err
	}

	r.sizeIndexBlock = int64(decodeUInt32(cellDefault[:]))
	nn += n
	startIndexBlock := r.size - r.sizeIndexBlock
	r.endDataBlock = startIndexBlock
	// end read header sparse index

	// start read sparse idx [key][data file offset]+[offsets key sparse idx]

	//fmt.Println(r.seqNum, r.lenKeys, r.size, r.sizeIndexBlock, r.size-r.sizeIndexBlock)
	r.buf.Reset()
	endOffsets := r.sizeIndexBlock - int64(nn)
	if r.buf.Len() < int(endOffsets) {
		r.buf.Grow(int(endOffsets))
	}
	buf = r.buf.Bytes()[:endOffsets]

	// load idx-block data in memory
	if n, err = r.fsst.ReadAt(buf, int64(startIndexBlock)); err != nil {
		return nil, err
	}
	if n < len(buf) {
		return nil, fmt.Errorf("read n %d < len buf %d", n, len(buf))
	}

	startOffset := endOffsets - (int64(r.lenKeys) * sizeCellDefault)
	r.offsets = r.buf.Bytes()[startOffset:endOffsets]
	r.keysvalues = r.buf.Bytes()[:startOffset]
	// end read sparse idx [key][data file offset]+[offsets key sparse idx]

	return r, nil
}

func (r *Reader) readOffsetAtDataBlock(pos int) (int64, error) {
	_, offset, err := r.readIdxBlockAt(pos)
	if err != nil {
		return 0, err
	}

	return int64(decodeUInt32(offset)), nil
}

func (r *Reader) readOffsetSparseKeyAt(pos int) int64 {
	return int64(decodeUInt32(r.offsets[pos*sizeCellDefault : pos*sizeCellDefault+sizeCellDefault]))
}

func (r *Reader) readIdxBlockAt(pos int) ([]byte, []byte, error) {
	offset := r.readOffsetSparseKeyAt(pos)
	kl, n := binary.Uvarint(r.keysvalues[offset:])
	offset += int64(n)

	vl, n := binary.Uvarint(r.keysvalues[offset:])
	offset += int64(n)

	key := r.keysvalues[offset : offset+int64(kl)]

	offset += int64(kl)
	val := r.keysvalues[offset : offset+int64(vl)]

	return key, val, nil
}

func (r *Reader) bsearch(skey []byte) (int64, int64, bool, error) {
	low, high, mid := 0, int(r.lenKeys), 0
	var from, to = int64(0), r.endDataBlock

	for low < high {
		mid = (low + high) / 2
		k, v, err := r.readIdxBlockAt(mid)
		if err != nil {
			return 0, 0, from < to, err
		}

		offset := int64(decodeUInt32(v))

		cmp := bytes.Compare(skey, k)
		switch cmp {
		// if skey == k
		case 0:
			from = offset

			if mid == int(r.lenKeys)-1 {
				return from, to, from < to, nil
			}

			to, err = r.readOffsetAtDataBlock(mid + 1)
			if err != nil {
				return 0, 0, from < to, err
			}

			return from, to, from < to, nil
		// if skey < k
		case -1:
			to = offset
			high = mid
		case 1:
			// if skey > k
			from = offset
			low = mid + 1
		}
	}

	return from, to, from < to, nil
}

func (r *Reader) readDataBlock(from, to int64) ([]byte, error) {
	block := make([]byte, to-from)
	n, err := r.fsst.ReadAt(block, from)
	if err != nil {
		return nil, err
	}

	if len(block) < n {
		return nil, fmt.Errorf("%d < %d", len(block), n)
	}

	return block, nil
}

func (r *Reader) search(key []byte) ([]byte, error) {
	from, to, ok, err := r.bsearch(key)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, ErrKeyNotFound
	}

	block, err := r.readDataBlock(from, to)
	if err != nil {
		return nil, err
	}

	return r.lsearch(key, block)
}

func (r *Reader) lsearch(skey, block []byte) ([]byte, error) {
	it, _, err := newBytesIterator(block)
	if err != nil {
		return nil, err
	}

	defer it.close()
	for it.hasNext() {
		key, val, _, err := it.next()

		if err != nil && err != io.EOF {
			return nil, err
		}
		if err == io.EOF {
			return nil, ErrKeyNotFound
		}
		if bytes.Equal(skey, key) {
			return val, nil
		}
	}

	return nil, ErrKeyNotFound
}
