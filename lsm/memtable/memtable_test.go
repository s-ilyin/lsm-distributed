package memtable

import (
	"testing"
)

func TestMemSwitch(t *testing.T) {
	mem := NewMem()
	mem.Put([]byte("a"), []byte("a"))

	sMem := mem.Switch()
	if _, ok := mem.Get([]byte("a")); ok {
		t.Fatal("key found!")
	}

	if _, ok := sMem.Get([]byte("a")); !ok {
		t.Fatal("key not found!")
	}

	mem.Put([]byte("b"), []byte("b"))
	if _, ok := mem.Get([]byte("b")); !ok {
		t.Fatal("key not found!")
	}
}
