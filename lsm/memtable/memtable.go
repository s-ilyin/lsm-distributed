package memtable

import sl "github.com/s-ilyin/lsm-distributed/lsm/skiplist"

type Memtable struct {
	data *sl.SkipList
	b    int
	len  int
}

// MemTable. All changes that are flushed to the WAL, but not flushed
// to the sorted files, are stored in memory for faster lookups.
// A red-black instance might be used directly, but the wrapper and additional
// layer of abstraction simplifies further changes.
func NewMem() *Memtable {
	return &Memtable{
		data: sl.NewSkipList(),
	}
}

// put puts the key and the value into the table.
func (mt *Memtable) Put(key, val []byte) {
	prev, ex := mt.data.Put(key, val)
	if ex {
		mt.b += -len(prev) + len(val)
	} else {
		mt.b += len(key) + len(val)
		mt.len++
	}
}

// get returns the value by the key.
// Caution! Get returns true for the removed keys in the memory.
func (mt *Memtable) Get(key []byte) ([]byte, bool) {
	return mt.data.Get(key)
}

func (mt *Memtable) Len() int {
	return mt.len
}

// bytes returns the size of all keys and values inserted into the MemTable in bytes.
func (mt *Memtable) Size() uint32 {
	return uint32(mt.b)
}

func (mt *Memtable) Switch() Memtable {
	old := *mt
	mt.data = sl.NewSkipList()
	mt.b = 0
	mt.len = 0

	return old
}

// clear clears all the data and resets the size.
func (mt *Memtable) Clear() {
	mt.data = sl.NewSkipList()
	mt.b = 0
}

// iterator returns iterator for the MemTable. It also iterates over
// deleted keys, but the value for them is nil.
func (mt *Memtable) Iterator() *MemTableIterator {
	return &MemTableIterator{
		it: mt.data.Iterator(),
	}
}

type MemTableIterator struct {
	it *sl.Iterator
}

func (it *MemTableIterator) HasNext() bool {
	return it.it.HasNext()
}

func (it *MemTableIterator) Next() ([]byte, []byte) {
	return it.it.Next()
}
