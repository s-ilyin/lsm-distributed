package sst

import (
	"fmt"
	"math"
	"path"
	"sync"
)

func NewFilesObserver(root string) (*ObserverFiles, error) {
	of := &ObserverFiles{
		dir: root,
	}
	if err := of.loadup(); err != nil {
		return nil, err
	}

	return of, nil
}

const maxLevel = math.MaxUint8

type Level uint8

const (
	BaseLevel Level = iota
)

type ObserverFiles struct {
	lock   sync.RWMutex
	levels [maxLevel]*SSTLevel
	dir    string
}

func (of *ObserverFiles) MaxLevel() Level {
	var lvl Level
	for idx := range of.levels {
		if of.levels[idx] != nil {
			lvl++
		}
	}

	return lvl
}

func (of *ObserverFiles) Levels() Level {
	return Level(len(of.levels))
}

func (of *ObserverFiles) Level(level Level) []File {
	of.lock.RLock()
	defer of.lock.RUnlock()

	if level <= maxLevel && of.levels[level] != nil {
		return of.levels[level].Files[:]
	}

	return []File{}
}

func (of *ObserverFiles) Len(level Level) int {
	of.lock.RLock()
	defer of.lock.RUnlock()
	return len(of.levels[level].Files[:])
}

func (of *ObserverFiles) Size(level Level) int64 {
	of.lock.RLock()
	defer of.lock.RUnlock()
	var size int64
	if level <= maxLevel && of.levels[level] != nil {
		files := of.levels[level].Files[:]

		for idx := range files {
			rd := files[idx].Reader
			size += rd.size
		}
	}

	return size

}

func (of *ObserverFiles) Append(level Level, file File) {
	if level <= maxLevel {
		of.lock.Lock()
		defer of.lock.Unlock()
		if of.levels[level] == nil {
			of.levels[level] = &SSTLevel{Files: make([]File, 0)}
			of.levels[level].Files = append(of.levels[level].Files, file)

			return
		}

		of.levels[level].Files = append(of.levels[level].Files, file)
	}
}

func (of *ObserverFiles) Flush(level Level) int {
	var n int
	if level <= maxLevel && of.levels[level] != nil {
		of.lock.Lock()
		n = len(of.levels[level].Files)
		of.levels[level].Files = of.levels[level].Files[:0]
		of.lock.Unlock()
	}

	return n
}

func (of *ObserverFiles) loadup() error {
	for lvl := Level(0); lvl < maxLevel; lvl++ {
		if err := of.Reload(lvl); err != nil {
			return err
		}
	}

	return nil
}

func (of *ObserverFiles) Reload(level Level) error {
	of.lock.Lock()
	defer of.lock.Unlock()
	files, err := filenames(PathForLevel(of.dir, level))
	if err != nil {
		return err
	}
	if of.levels[level] == nil {
		of.levels[level] = &SSTLevel{}
	}

	for idx := range files {
		//fmt.Println("open file", path.Join(PathForLevel(of.dir, level), files[idx]))
		r, err := NewReader(path.Join(PathForLevel(of.dir, level), files[idx]))
		if err != nil {
			return err
		}
		of.levels[level].Files = append(of.levels[level].Files, File{
			Reader: r,
		})
	}

	//log.Println("update lvl", len(of.levels[level].Files))
	return nil
}

func (of *ObserverFiles) NewNext(level Level) string {
	return fmt.Sprintf("000%d.sst", len(of.levels[level].Files))
}

func (of *ObserverFiles) Iterator(max Level) *LevelIterator {
	return newLevelIterator(of.levels[:max])
}
