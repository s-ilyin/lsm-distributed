package sst

import (
	"bufio"
	"io"
	"log"
	"os"
	"path"
	"slices"
	"testing"
)

func Test_EncodeDecode(t *testing.T) {
	var dir = "tmp-test-encode-decode"
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, os.FileMode(0700))
	}
	defer os.RemoveAll(dir)

	file := "encode_decode.sst"
	f, err := os.OpenFile(path.Join(dir, file), os.O_CREATE|os.O_RDWR, os.FileMode(0600))
	if err != nil {
		t.Fatal(err)
	}

	var key = [3]byte{100, 101, 102}
	var val = [3]byte{200, 201, 202}

	tests := []struct {
		name string
		key  []byte
		val  []byte
		i    *bufio.Writer
		o    *bufio.Reader
	}{
		{
			name: "ok",
			key:  key[:],
			val:  val[:],
			i:    bufio.NewWriter(f),
			o:    bufio.NewReader(f),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if n, err := Encode(tt.i, tt.key, tt.val); err != nil {
				t.Fatal(err)
			} else {
				if n == 0 {
					t.Fatal("n == 0")
				}
				//log.Println("n write ", n)
			}
			tt.i.Flush()

			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			f, err = os.OpenFile(path.Join(dir, file), os.O_CREATE|os.O_RDWR, os.FileMode(0600))
			if err != nil {
				t.Fatal(err)
			}
			tt.o = bufio.NewReader(f)

			if k, v, err := Decode(tt.o); err != nil && err != io.EOF {
				t.Fatal(err)
			} else {
				log.Println(string(tt.key), string(k))
				if !slices.Equal(tt.key, k) {
					t.Fatalf("%s != %s", string(tt.key), string(k))
				}
				if !slices.Equal(tt.val, v) {
					t.Fatalf("%s != %s", string(tt.val), string(v))
				}
			}
		})
	}
}
