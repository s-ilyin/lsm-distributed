package sst

import (
	"bytes"
	"container/heap"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/s-ilyin/lsm-distributed/lsm/bloom"
	"github.com/s-ilyin/lsm-distributed/lsm/encoder"
)

type iterator struct {
	n      int
	it     *FileIterator
	seqNum uint64
}

func push(h *Heap, it *iterator) {
	if has := it.it.HasNext(); has {
		//fmt.Println("push", it.n, it.it.HasNext())
		if k, v, err := it.it.Next(); err == nil {
			heap.Push(h, &Node{Seq: it.seqNum, SST: ElemSST{Key: k, Val: v}, It: it})
		} else {
			log.Println("err push heap", string(k), string(v), err)
		}
	}
}

func pop(h *Heap) *Node {
	return heap.Pop(h).(*Node)
}

func Compact(dirname string, files []*Reader, size int64, distance int32, rm bool) (string, error) {
	hp := &Heap{}
	heap.Init(hp)
	var (
		maxSeqNum    uint64 = 0
		maxCountKeys int
		mergepath    string
	)

	for idx := range files {
		r := files[idx]

		//defer r.Close()
		it, err := r.Iterator()
		if err != nil {
			return mergepath, fmt.Errorf("open iterator %s", err)
		}

		if r.Sequence() > maxSeqNum {
			maxSeqNum = r.Sequence()
		}
		maxCountKeys += int(r.lenKeys)*int(distance) + int(distance)

		push(hp, &iterator{it: it, seqNum: r.Sequence(), n: idx})
	}
	if hp.Len() == 0 {
		return mergepath, nil
	}
	mergepath = path.Join(dirname, "level-merge")
	if _, err := os.Stat(mergepath); os.IsNotExist(err) {
		if err := os.Mkdir(mergepath, os.FileMode(0700)); err != nil {
			return mergepath, err
		}
	}

	// filename, err := NextFilename(mergepath)
	// if err != nil {
	// 	return "", nil, err
	// }
	filename := NewNext()

	wr, err := NewWriter(path.Join(mergepath, filename), SparseKeyDistance(distance))
	if err != nil {
		return mergepath, fmt.Errorf("open writer %s", err)
	}

	var (
		ssts    = make([]File, 0)
		decoder = encoder.NewDecoder()
		filter  = bloom.New(maxCountKeys, 100)
	)

	wf := func(n *Node) error {
		if decoder.Decode(n.SST.Val).IsTombstone() && rm {
			return nil
		}

		if wr.Bytes() > int(size) {
			if err = wr.AddIdxBlock(n.Seq); err != nil {
				return fmt.Errorf("add idx block %s", err)
			}
			if err = wr.Close(); err != nil {
				return fmt.Errorf("close writer %s", err)
			}

			rd, err := wr.Reader()
			if err != nil {
				return err
			}

			sst := NewCache(rd, *filter)
			ssts = append(ssts, sst)
			filter = bloom.New(maxCountKeys, 100)
			filename = NewNext()

			wr, err = NewWriter(path.Join(mergepath, filename), SparseKeyDistance(distance))
			if err != nil {
				return fmt.Errorf("open writer %s", err)
			}
		}

		filter.AddByte(n.SST.Key)
		return wr.Write(n.SST.Key, n.SST.Val)
	}

	var (
		cur  = pop(hp)
		next *Node
	)
	push(hp, cur.It)

	for hp.Len() > 0 {
		next = pop(hp)
		push(hp, next.It)
		if cur != nil && bytes.Equal(cur.SST.Key, next.SST.Key) {
			if next.Seq > cur.Seq {
				cur = next
			}
			continue
		}
		if err := wf(cur); err != nil {
			return mergepath, fmt.Errorf("err write %s", err)
		}

		cur = next
	}

	if err := wf(cur); err != nil {
		return mergepath, fmt.Errorf("err write %s", err)
	}

	if err := wr.AddIdxBlock(cur.Seq); err != nil {
		return mergepath, fmt.Errorf("add idx block %s", err)
	}

	if err := wr.Close(); err != nil {
		return mergepath, fmt.Errorf("close writer %s", err)
	}

	return mergepath, nil
}
