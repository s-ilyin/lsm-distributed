package sst

import (
	"bytes"
	"os"
	"path"
	"strconv"
	"testing"
)

func TestLevelIterator(t *testing.T) {
	test := [maxLevel]*SSTLevel{
		{
			Files: []File{
				{
					Name: "2",
				},
				{
					Name: "1",
				},
				{
					Name: "0",
				},
			},
		},
		{
			Files: []File{
				{
					Name: "6",
				},
				{
					Name: "5",
				},
				{
					Name: "4",
				},
				{
					Name: "3",
				},
			},
		},
		{
			Files: []File{
				{
					Name: "11",
				},
				{
					Name: "10",
				},
				{
					Name: "9",
				},
				{
					Name: "8",
				},
				{
					Name: "7",
				},
			},
		},
	}
	var want int
	for idx := range test {
		if test[idx] != nil {
			want += len(test[idx].Files)
		}
	}
	var expect int
	numFiles := 0
	it := newLevelIterator(test[:3])
	for it.hasNext() {
		file := it.next()
		if file.Name == strconv.Itoa(numFiles) {
			numFiles++
		} else {
			t.Fatalf("want %d expect %s", numFiles, file.Name)
		}
		expect++
	}
	if want != expect {
		t.Fatalf("want %d expect %d", want, expect)
	}
}

func TestBytesIterator(t *testing.T) {
	var dir = "tmp-test-bytes-iterator"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.FileMode(0700))
	}
	defer os.RemoveAll(dir)

	filename, err := NextFilename(dir)
	if err != nil {
		t.Fatal(err)
	}

	wr, err := NewWriter(path.Join(dir, filename), SparseKeyDistance(4))
	if err != nil {
		t.Fatal(err)
	}

	test := []struct {
		k []byte
		v []byte
	}{
		{
			k: []byte("aa"),
			v: []byte("bb"),
		},
		{
			k: []byte("cc"),
			v: []byte("dd"),
		},
	}

	for idx := range test {
		wr.Write(test[idx].k, test[idx].v)
	}

	wr.AddIdxBlock(10)
	wr.Close()

	rd, err := wr.Reader()
	if err != nil {
		t.Fatal(err)
	}

	block, err := rd.readDataBlock(0, rd.endDataBlock)
	if err != nil {
		t.Fatal(err)
	}

	it, _, err := newBytesIterator(block)
	if err != nil {
		t.Fatal(err)
	}

	var i int
	for it.hasNext() {
		tk := test[i].k
		tv := test[i].v
		k, v, _, err := it.next()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(tk, k) {
			t.Fatalf("[key] want %s expect %s", string(tk), string(k))
		}
		if !bytes.Equal(tv, v) {
			t.Fatalf("[val] want %s expect %s", string(tv), string(v))
		}
		i++
	}
	if i != len(test) {
		t.Fatalf("want %d op expect %d op", len(test), i)
	}
}

func TestFileIteratorOneSegment(t *testing.T) {
	var dir = "tmp-test-file-iterator-one-segment"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.FileMode(0700))
	}
	defer os.RemoveAll(dir)

	filename, err := NextFilename(dir)
	if err != nil {
		t.Fatal(err)
	}

	wr, err := NewWriter(path.Join(dir, filename), SparseKeyDistance(2000))
	if err != nil {
		t.Fatal(err)
	}

	test := []struct {
		k []byte
		v []byte
	}{
		{
			k: []byte("a"),
			v: []byte("aa"),
		},
		{
			k: []byte("b"),
			v: []byte("bb"),
		},
		{
			k: []byte("c"),
			v: []byte("cc"),
		},
		{
			k: []byte("d"),
			v: []byte("dd"),
		},
		{
			k: []byte("e"),
			v: []byte("ee"),
		},
		{
			k: []byte("f"),
			v: []byte("ff"),
		},
		{
			k: []byte("g"),
			v: []byte("gg"),
		},
		{
			k: []byte("h"),
			v: []byte("hh"),
		},
		{
			k: []byte("i"),
			v: []byte("ii"),
		},
		{
			k: []byte("j"),
			v: []byte("jj"),
		},
		{
			k: []byte("k"),
			v: []byte("kk"),
		},
		{
			k: []byte("l"),
			v: []byte("mm"),
		},
		{
			k: []byte("n"),
			v: []byte("nn"),
		},
	}

	for idx := range test {
		wr.Write(test[idx].k, test[idx].v)
	}

	wr.AddIdxBlock(10)
	wr.Close()

	rd, err := wr.Reader()
	if err != nil {
		t.Fatal(err)
	}
	it, err := rd.Iterator()
	if err != nil {
		t.Fatal(err)
	}

	var i int
	for it.HasNext() {
		tk := test[i].k
		tv := test[i].v
		k, v, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(tk, k) {
			t.Fatalf("want key %s expect %s", string(tk), string(k))
		}
		if !bytes.Equal(tv, v) {
			t.Fatalf("want val %s expect %s", string(tv), string(v))
		}
		i++
	}
	if i != len(test) {
		t.Fatalf("want %d op expect %d op", len(test), i)
	}
}

func TestFileIteratorManySegments(t *testing.T) {
	var dir = "tmp-test-file-iterator-many-segments"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.FileMode(0700))
	}
	defer os.RemoveAll(dir)

	filename, err := NextFilename(dir)
	if err != nil {
		t.Fatal(err)
	}

	wr, err := NewWriter(path.Join(dir, filename), SparseKeyDistance(3))
	if err != nil {
		t.Fatal(err)
	}

	test := []struct {
		k []byte
		v []byte
	}{
		{
			k: []byte("a"),
			v: []byte("aa"),
		},
		{
			k: []byte("b"),
			v: []byte("bb"),
		},
		{
			k: []byte("c"),
			v: []byte("cc"),
		},
		{
			k: []byte("d"),
			v: []byte("dd"),
		},
		{
			k: []byte("e"),
			v: []byte("ee"),
		},
		{
			k: []byte("f"),
			v: []byte("ff"),
		},
		{
			k: []byte("g"),
			v: []byte("gg"),
		},
		{
			k: []byte("h"),
			v: []byte("hh"),
		},
		{
			k: []byte("i"),
			v: []byte("ii"),
		},
		{
			k: []byte("j"),
			v: []byte("jj"),
		},
		{
			k: []byte("k"),
			v: []byte("kk"),
		},
		{
			k: []byte("l"),
			v: []byte("mm"),
		},
		{
			k: []byte("n"),
			v: []byte("nn"),
		},
	}

	for idx := range test {
		wr.Write(test[idx].k, test[idx].v)
	}

	wr.AddIdxBlock(10)
	wr.Close()

	rd, err := wr.Reader()
	if err != nil {
		t.Fatal(err)
	}
	it, err := rd.Iterator()
	if err != nil {
		t.Fatal(err)
	}

	var i int
	for it.HasNext() {
		tk := test[i].k
		tv := test[i].v
		k, v, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(tk, k) {
			t.Fatalf("want key %s expect %s", string(tk), string(k))
		}
		if !bytes.Equal(tv, v) {
			t.Fatalf("want val %s expect %s", string(tv), string(v))
		}
		i++
	}
	if i != len(test) {
		t.Fatalf("want %d op expect %d op", len(test), i)
	}
}
