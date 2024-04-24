package sst

import (
	"bytes"
	"container/heap"
	"log"
	"testing"
)

func Test_HeapPush(t *testing.T) {

	tests := []struct {
		name   string
		seqNum uint64
		Key    []byte
		Val    []byte
	}{
		{
			name:   "key a",
			seqNum: 0,
			Key:    []byte("a"),
			Val:    []byte("a"),
		},
		{
			name:   "key a",
			seqNum: 1,
			Key:    []byte("a"),
			Val:    []byte("a"),
		},
		{
			name:   "key b",
			seqNum: 2,
			Key:    []byte("b"),
			Val:    []byte("b"),
		},
		{
			name:   "key c",
			seqNum: 3,
			Key:    []byte("c"),
			Val:    []byte("c"),
		},
		{
			name:   "key c",
			seqNum: 4,
			Key:    []byte("c"),
			Val:    []byte("c"),
		},
	}
	hp := &Heap{}
	heap.Init(hp)

	var push = func(h *Heap, seqNum uint64, elem ElemSST) {
		heap.Push(h, &Node{Seq: seqNum, SST: elem})
	}
	var pop = func(h *Heap) *Node {
		return heap.Pop(h).(*Node)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			push(hp, tt.seqNum, ElemSST{Key: tt.Key, Val: tt.Val})
		})
	}

	n := pop(hp)
	if !bytes.Equal(tests[0].Key, n.SST.Key) {
		t.Fatalf("tests[0].Key %s != n.SST.Key %s", string(tests[0].Key), string(n.SST.Key))
	}

	for hp.Len() > 0 {
		n := pop(hp)
		log.Println(n.Seq, string(n.SST.Key))
	}
}
