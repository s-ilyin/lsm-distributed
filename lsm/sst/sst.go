package sst

import "github.com/s-ilyin/lsm-distributed/lsm/bloom"

type SSTLevel struct {
	Files []File
}

func NewCache(reader *Reader, filter bloom.Filter) File {
	return File{
		Filter: filter,
		Reader: reader,
	}
}

type File struct {
	Name   string
	Reader *Reader
	Filter bloom.Filter
}

type ElemSST struct {
	Key, Val []byte
}
