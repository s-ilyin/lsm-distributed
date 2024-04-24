package sl

import (
	"bytes"
	"math"

	"github.com/s-ilyin/lsm-distributed/lsm/fastrand"
)

// https://www.cloudcentric.dev/implementing-a-skip-list-in-go/

const (
	MaxHeight = 16
	PValue    = 0.5 // p = 1/2
)

var probabilities [MaxHeight]uint32

// ...

func init() {
	probability := 1.0

	for level := 0; level < MaxHeight; level++ {
		probabilities[level] = uint32(probability * float64(math.MaxUint32))
		probability *= PValue
	}
}

func NewSkipList() *SkipList {
	sl := &SkipList{}
	sl.head = &node{}
	sl.height = 1

	return sl
}

type node struct {
	key   []byte
	val   []byte
	tower [MaxHeight]*node
}

type SkipList struct {
	head   *node
	height int
	size   int32
}

func randomHeight() int {
	seed := fastrand.Uint32()

	height := 1
	for height < MaxHeight && seed <= probabilities[height] {
		height++
	}

	return height
}

func (sl *SkipList) Put(key []byte, val []byte) ([]byte, bool) {
	found, journey := sl.search(key)

	if found != nil {
		prev := found.val

		// update value of existing key
		found.val = val

		return prev, true
	}
	height := randomHeight()
	nd := &node{key: key, val: val}

	for level := 0; level < height; level++ {
		prev := journey[level]

		if prev == nil {
			// prev is nil if we are extending the height of the tree,
			// because that level did not exist while the journey was being recorded
			prev = sl.head
		}
		nd.tower[level] = prev.tower[level]
		prev.tower[level] = nd
	}

	if height > sl.height {
		sl.height = height
	}
	sl.size++

	return nil, false
}

func (sl *SkipList) search(key []byte) (*node, [MaxHeight]*node) {
	var next *node
	var journey [MaxHeight]*node

	prev := sl.head
	for level := sl.height - 1; level >= 0; level-- {
		for next = prev.tower[level]; next != nil; next = prev.tower[level] {
			// если key < или == next.key
			if bytes.Compare(key, next.key) <= 0 {
				break
			}
			prev = next
		}

		journey[level] = prev
	}

	if next != nil && bytes.Equal(key, next.key) {
		return next, journey
	}

	return nil, journey
}

func (sl *SkipList) Delete(key []byte) bool {
	found, journey := sl.search(key)

	if found == nil {
		return false
	}

	for level := 0; level < sl.height; level++ {
		if journey[level].tower[level] != found {
			break
		}
		journey[level].tower[level] = found.tower[level]
		found.tower[level] = nil
	}
	found = nil
	sl.shrink()

	return true
}

// выравнивает высоту
func (sl *SkipList) shrink() {
	for level := sl.height - 1; level >= 0; level-- {
		if sl.head.tower[level] == nil {
			sl.height--
		}
	}
}

func (sl *SkipList) String() string {
	v := &visualizer{sl}
	return v.visualize()
}

func (sl *SkipList) Get(key []byte) ([]byte, bool) {
	found, _ := sl.search(key)

	if found == nil {
		return nil, false
	}

	return found.val, true
}

func (sl *SkipList) Size() int32 {
	return sl.size
}
