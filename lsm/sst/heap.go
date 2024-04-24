package sst

import "bytes"

type Node struct {
	Seq uint64
	SST ElemSST
	It  *iterator
}

// An min-heap of SST entries
// Provides an easy way to sort large numbers of entries
type Heap []*Node

func (h Heap) Len() int { return len(h) }
func (h Heap) Less(i, j int) bool {
	return bytes.Compare(h[i].SST.Key, h[j].SST.Key) < 1
}
func (h Heap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*Node))
}

func (h *Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
