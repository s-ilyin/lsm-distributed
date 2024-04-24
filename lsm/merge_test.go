package lsm

import (
	"os"
	"testing"
	"time"
)

func TestMerge(t *testing.T) {
	var dir = "tmp-test-merge"
	l, err := Open(dir, MemTableThreshold(6), DebugMode(true), MergeConfig(MergeSettings{
		Interval:         1 * time.Second,
		NumberOfSstFiles: 4,
		MaxLevels:        4,
		DataSize:         128,
	}))

	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	defer l.Shutdown()

	l.Put([]byte("a"), []byte("aa"))
	l.Put([]byte("b"), []byte("bb"))

	//l.logger.Debug("flush")
	l.Put([]byte("c"), []byte("cc"))
	l.Put([]byte("d"), []byte("dd"))

	l.Put([]byte("e"), []byte("ee"))
	l.Put([]byte("f"), []byte("ff"))

	l.Put([]byte("g"), []byte("gg"))
	l.Put([]byte("h"), []byte("hh"))
	l.Put([]byte("g"), []byte("gg"))

	l.Put([]byte("h"), []byte("hh"))
	l.Put([]byte("i"), []byte("ii"))

	l.Put([]byte("j"), []byte("jj"))
	l.Put([]byte("k"), []byte("kk"))

	l.Put([]byte("l"), []byte("ll"))
	l.Put([]byte("m"), []byte("mm"))

	l.Put([]byte("n"), []byte("nn"))
	l.Put([]byte("o"), []byte("oo"))

	l.Put([]byte("p"), []byte("pp"))
	l.Put([]byte("q"), []byte("qq"))
	l.Put([]byte("v"), []byte("vv"))
	l.Put([]byte("w"), []byte("ww"))
	l.Put([]byte("z"), []byte("zz"))

	time.Sleep(2 * time.Second)
}
