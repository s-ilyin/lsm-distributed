package sst

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"testing"
)

func TestOffsetsReader(t *testing.T) {
	var dir = "tmp-test-offset-reader"
	test := []struct {
		name   string
		offset int64
	}{
		{
			name:   "1",
			offset: 0,
		},
		{
			name:   "2",
			offset: 8,
		},
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, os.FileMode(0777))
	}
	defer os.RemoveAll(dir)

	wr, err := NewWriter(path.Join(dir, "0000.sst"), SparseKeyDistance(4))
	if err != nil {
		t.Fatal(err)
	}

	wr.Write([]byte("aa"), []byte("bb"))
	wr.Write([]byte("cc"), []byte("dd"))

	wr.AddIdxBlock(10)
	wr.Close()

	rd, err := NewReader(wr.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()

	for idx := 0; idx < int(rd.lenKeys); idx++ {
		offset := rd.readOffsetSparseKeyAt(idx)
		tt := test[idx]
		if tt.offset != offset {
			t.Fatalf("want %d expect %d", tt.offset, offset)
		}
	}
}

func TestReader(t *testing.T) {
	var dir = "tmp-test-reader"
	tests := []struct {
		name string
		key  []byte
		val  []byte
	}{
		{
			name: "1",
			key:  []byte("a"),
			val:  []byte("aaaaa"),
		},
		{
			name: "2",
			key:  []byte("b"),
			val:  []byte("bbbbb"),
		},
		{
			name: "3",
			key:  []byte("c"),
			val:  []byte("ccccc"),
		},
		{
			name: "4",
			key:  []byte("d"),
			val:  []byte("ddddd"),
		},
		{
			name: "5",
			key:  []byte("e"),
			val:  []byte("eeeee"),
		},
		{
			name: "6",
			key:  []byte("f"),
			val:  []byte("fffff"),
		},
		{
			name: "7",
			key:  []byte("g"),
			val:  []byte("ggggg"),
		},
	}

	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, os.FileMode(0777))
	}
	defer os.RemoveAll(dir)

	wr, err := NewWriter(path.Join(dir, "0000.sst"), SparseKeyDistance(8))
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wr.Write(tt.key, tt.val)
		})
	}
	if err := wr.AddIdxBlock(1); err != nil {
		t.Fatal(err)
	}
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(wr.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if val, err := r.search(tt.key); err != nil && err != ErrKeyNotFound {
				t.Fatal(err)
			} else {
				if err == ErrKeyNotFound {
					t.Fatal(string(tt.key), val, ErrKeyNotFound)
				}
				if !bytes.Equal(tt.val, val) {
					t.Fatalf("%s != %s", tt.val, val)
				}
			}
		})
	}
}

var rootDir = "bench-tmp"

func BenchmarkReaderSparse2048(b *testing.B) {
	keys := []string{
		//"000000ad-e328-41fb-ac65-d2a9347baa90",
		"ff22bdad-8fbe-46f8-bfdc-ca6dda8ec833",
		//"ff0a539c-d2a6-493c-9058-428e367080e4",
	}

	r, err := NewReader(path.Join(rootDir, fmt.Sprintf("000%d.sst", 0)))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for idx := range keys {
			_, err = r.search([]byte(keys[idx]))
			if err != nil {
				panic(fmt.Errorf("%s %s", []byte(keys[idx]), err))
			}
		}
	}
}

func BenchmarkReaderSparse4096(b *testing.B) {
	keys := []string{
		//"00bd6778-0fb1-4547-9c9c-f1e5565a56f5",
		"06c4e379-ba01-4647-9a9f-beb7f15c3cf5",
		// "fba2568f-e151-47bb-b8f4-c6d09a9342f9",
	}
	r, err := NewReader(path.Join(rootDir, fmt.Sprintf("000%d.sst", 1)))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for idx := range keys {
			_, err = r.search([]byte(keys[idx]))
			if err != nil {
				panic(fmt.Errorf("%s %s", []byte(keys[idx]), err))
			}
		}

	}
}

func BenchmarkReaderSparse8192(b *testing.B) {
	keys := []string{
		"112479e3-44b4-47ab-aac0-15ee3b8c7b89",
		// "ad069aa7-ef15-409c-9474-a47ad5180a98",
		// "fdeec487-4949-4a46-9426-84e7269e8841",
	}
	r, err := NewReader(path.Join(rootDir, fmt.Sprintf("000%d.sst", 2)))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for idx := range keys {
			_, err = r.search([]byte(keys[idx]))
			if err != nil {
				panic(fmt.Errorf("%s %s", []byte(keys[idx]), err))
			}
		}
	}
}

func BenchmarkReaderSparse16384(b *testing.B) {
	keys := []string{
		//"03822da8-dcb1-495d-ac09-3f30446655d1",
		"5df43423-aaf3-4567-ba56-785ffdd157b6",
		// "f58fa0a5-c567-4146-9ac7-ebf3fcaf9183",
	}
	r, err := NewReader(path.Join(rootDir, fmt.Sprintf("000%d.sst", 3)))
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for idx := range keys {
			_, err = r.search([]byte(keys[idx]))
			if err != nil {
				panic(fmt.Errorf("%s %s", []byte(keys[idx]), err))
			}
		}
	}
}
