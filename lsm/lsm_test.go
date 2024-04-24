package lsm

import (
	"bytes"
	"os"
	"testing"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
)

type kv struct {
	k []byte
	v []byte
}

func value(size int) []byte {
	val := make([]byte, 0, size)
	for len(val) < size {
		w := faker.Word()
		for idx := range w {
			val = append(val, byte(w[idx]))
			if len(val) == size {
				return val
			}
		}
	}

	return val
}

func prepareData(l *LSMTree) []kv {
	var data []kv

	for idx := 0; idx < 100; idx++ {
		key := []byte(uuid.NewString())
		val := value(1 << 5)
		if err := l.Put(key, val); err != nil {
			panic(err)
		}

		data = append(data, kv{
			k: key,
			v: val,
		})
	}

	return data
}

func TestGet(t *testing.T) {
	var dir = "lsm-get-put"
	l, err := Open(dir, MemTableThreshold(4), DebugMode(true))
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	defer l.Shutdown()

	data := prepareData(l)
	for idx := range data {
		v, ok, err := l.Get(data[idx].k)
		if err != nil {
			t.Fatalf("[err] want %s expext %s", "", err.Error())
		}
		if !ok {
			t.Fatalf("[ok] want true expect %v", ok)
		}
		if !bytes.Equal(data[idx].v, v) {
			t.Fatalf("[val] want %s expect %s", string(data[idx].v), string(v))
		}
	}
}
