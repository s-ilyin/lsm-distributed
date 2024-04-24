package sst

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	newflags = os.O_WRONLY | os.O_CREATE
)

// Levels возвращает имена любых каталогов, содержащих консолидированные
// Файлы SST на уровнях, превышающих уровень 0. Это означает, что данные
// организованы в неперекрывающиеся области между файлами на этом уровне.
func Levels(path string) ([]string, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	var lvls []string
	matched, err := regexp.Compile(`^level-[0-9]*`)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		if matched.Match([]byte(file.Name())) && file.IsDir() {
			lvls = append(lvls, file.Name())
		}
	}

	return lvls, nil
}

func PathForLevel(base string, level Level) string {
	return fmt.Sprintf("%s/level-%d", base, level)
}

// Filenames возвращает имена бинарных файлов SST по пути
func filenames(path string) ([]string, error) {
	var sstFiles []string
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, nil
	}
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	matched, err := regexp.Compile(`^data_[-a-z0-9]*\.sst`)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if matched.Match([]byte(file.Name())) && !file.IsDir() {
			sstFiles = append(sstFiles, file.Name())
		}
	}

	return sstFiles, nil
}

func NewSSTFiles(filepath string) (*os.File, error) {
	df, err := os.OpenFile(filepath, newflags, os.FileMode(0700))
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filepath, err)
	}

	return df, nil
}

var mu sync.Mutex

// NextFilename returns the name of the next SST binary file in given directory
func NextFilename(path string) (string, error) {
	mu.Lock()
	defer mu.Unlock()
	files, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}

	var sstFiles []string
	matched, err := regexp.Compile(`^[0-9]*\.sst`)
	if err != nil {
		return "", err
	}

	for _, file := range files {
		if matched.Match([]byte(file.Name())) && !file.IsDir() {
			sstFiles = append(sstFiles, file.Name())
		}
	}

	if len(sstFiles) > 0 {
		var latest = sstFiles[len(sstFiles)-1][4:8]
		n, _ := strconv.Atoi(latest)
		return fmt.Sprintf("%04d.sst", n+1), nil
	}

	return "0000.sst", nil
}

func NewNext() string {
	ts := time.Now().UTC().Unix()
	return fmt.Sprintf("data_%s-%d.sst", uuid.NewString(), ts)
}

func OpenBy(binpath string) (*os.File, error) {
	var (
		err  error
		fsst *os.File
	)
	fsst, err = os.OpenFile(binpath, os.O_RDONLY, os.FileMode(0600))
	if err != nil {
		return nil, err
	}

	return fsst, nil
}

// searchInDiskTables searches a value by the key in DiskTables, by traversing
// all tables in the directory.
func SearchInDiskTables(key []byte, iterator *LevelIterator) ([]byte, bool, error) {
	var err error
	for iterator.hasNext() {
		var val []byte
		file := iterator.next()
		val, err = searchInDiskTable(key, file.Reader)
		if err != nil && err != ErrKeyNotFound {
			return nil, false, fmt.Errorf("failed to search in disk table %s: %s", file.Reader.Name(), err)
		}
		if err == ErrKeyNotFound {
			continue
		}

		return val, true, nil
	}

	return nil, false, err
}

// searchInDiskTable searches a given key in a given disk table.
func searchInDiskTable(key []byte, reader *Reader) ([]byte, error) {
	return reader.search(key)
}
