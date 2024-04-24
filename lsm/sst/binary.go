package sst

import (
	"encoding/binary"
	"fmt"
	"io"
)

type IOReader interface {
	io.Reader
	io.ByteReader
}

// encode encodes key and value and writes it to the specified writer.
// Returns the number of bytes written and error if occurred.
// The function must be compatible with decode: encode(decode(v)) == v.
func Encode(w io.Writer, key []byte, value []byte) (int, error) {
	// encoding format:
	// [encoded key length in bytes][encoded value length in bytes][key][value]

	// number of bytes written
	off := 0
	kl, vl := len(key), len(value)

	kvLen := 2*binary.MaxVarintLen64 + kl + vl
	buf := make([]byte, kvLen)

	off += binary.PutUvarint(buf, uint64(kl))
	off += binary.PutUvarint(buf[off:], uint64(vl))

	n := copy(buf[off:], key)
	if n < kl {
		return off + n, fmt.Errorf("n %d < kl %d", n, kl)
	}
	off += n

	n = copy(buf[off:], value)
	if n < vl {
		return off + n, fmt.Errorf("n %d < vl %d", n, vl)
	}
	off += n

	var err error
	if n, err = w.Write(buf[:off]); err != nil {
		return off + n, err
	}
	if n < off {
		return off, fmt.Errorf("write %d < off %d", n, off)
	}

	return off, nil
}

// decode decodes key and value by reading from the specified reader.
// Returns the number of bytes read and error if occurred.
// The function must be compatible with encode: encode(decode(v)) == v.
func Decode(r IOReader) ([]byte, []byte, error) {
	// encoding format:
	// [encoded key length in bytes][encoded value length in bytes][key][value]

	kl, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, nil, err
	}
	vl, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, nil, err
	}
	keyval := make([]byte, kl+vl)
	n, err := r.Read(keyval)
	if err != nil {
		return nil, nil, err
	}
	if uint64(n) < kl {
		return nil, nil, fmt.Errorf("the file is corrupted, failed to read entry %d < %d", n, kl)
	}
	key := keyval[:kl]
	val := keyval[kl:]

	return key, val, nil
}

// encodeKeyOffset encodes key offset and writes it to the given writer.
func EncodeKeyOffset(w io.Writer, key []byte, offset int) (int, error) {
	return Encode(w, key, encodeUInt32(uint32(offset)))
}

func binaryPutUint64(w io.Writer, x uint64) (int, error) {
	return w.Write(encodeUInt64(x))
}

func binaryUint64(r io.Reader) (uint64, error) {
	var decode [8]byte
	n, err := r.Read(decode[:])
	if err != nil {
		return 0, err
	}
	if n < len(decode) {
		return 0, fmt.Errorf("read %d less than required %d", n, len(decode))
	}

	return decodeUInt64(decode[:]), nil
}

// encodeInt encodes the int as a slice of bytes.
// Must be compatible with decodeInt.
func encodeUInt64(x uint64) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], x)

	return encoded[:]
}

// decodeInt decodes the slice of bytes as an int.
// Must be compatible with encodeInt.
func decodeUInt64(encoded []byte) uint64 {
	return binary.LittleEndian.Uint64(encoded)
}

func binaryUint32(r io.Reader) (uint64, error) {
	var decode [4]byte
	n, err := r.Read(decode[:])
	if err != nil {
		return 0, err
	}
	if n < len(decode) {
		return 0, fmt.Errorf("read %d less than required %d", n, len(decode))
	}

	return decodeUInt64(decode[:]), nil
}

func binaryPutUint32(w io.Writer, x uint32) (int, error) {
	return w.Write(encodeUInt32(x))
}

// decodeUInt32 decodes the slice of bytes as an int.
// Must be compatible with encodeInt.
func decodeUInt32(encoded []byte) uint32 {
	return binary.LittleEndian.Uint32(encoded)
}

// encodeUInt32 encodes the int as a slice of bytes.
// Must be compatible with decodeInt.
func encodeUInt32(x uint32) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], x)

	return encoded[:]
}

func WriteUInt32Pair(w io.Writer, x, y uint32) (int, error) {
	return w.Write(EncodeUint32Pair(x, y))
}

// encodeIntPair encodes two ints.
func EncodeUint32Pair(x, y uint32) []byte {
	var encoded [8]byte
	binary.LittleEndian.PutUint32(encoded[0:4], x)
	binary.LittleEndian.PutUint32(encoded[4:], y)

	return encoded[:]
}

// decodeIntPair decodes two ints.
func DecodeUint32Pair(encoded []byte) (uint32, uint32) {
	x := binary.LittleEndian.Uint32(encoded[0:4])
	y := binary.LittleEndian.Uint32(encoded[4:])

	return x, y
}
