package sl

import (
	"testing"
)

func TestSkipList(t *testing.T) {
	sl := NewSkipList()

	sl.Put([]byte("a"), []byte("a"))
	sl.Put([]byte("c"), []byte("c"))
	sl.Put([]byte("b"), []byte("b"))
}
