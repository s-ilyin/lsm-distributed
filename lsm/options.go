package lsm

import "time"

const (
	// Default MemTable table threshold.
	defaultMemTableThreshold = 64000 // 64 kB
	// Default distance between keys in sparse index.
	defaultSparseKeyDistance = 4 << 10
	// Default DiskTable number threshold.
	defaultDiskTableNumThreshold = 10
)

func DebugMode(debug bool) func(*LSMTree) {
	return func(l *LSMTree) {
		l.debug = debug
	}
}

// MemTableThreshold устанавливает порог memTable для дерева LSM.
// Если размер MemTable в байтах превышает пороговое значение, он должен
// быть сброшен на диск.
func MemTableThreshold(memTableThreshold uint32) func(*LSMTree) {
	return func(t *LSMTree) {
		t.config.MemtblDataSize = memTableThreshold
	}
}

// SparseKeyDistance устанавливает расстояние между разреженными ключами для дерева LSM.
// Расстояние между ключами в разреженном индексе.
func SparseKeyDistance(sparseKeyDistance int32) func(*LSMTree) {
	return func(t *LSMTree) {
		t.sparseKeyDistance = sparseKeyDistance
	}
}

// DiskTableNumThreshold устанавливает diskTableNumThreshold для дерева LSM.
// Если номер дисковой таблицы превышает пороговое значение, дисковые таблицы должны быть
// объединены, чтобы уменьшить его.
func DiskTableNumThreshold(diskTableNumThreshold int) func(*LSMTree) {
	return func(t *LSMTree) {
		t.diskTableNumThreshold = diskTableNumThreshold
	}
}

func DiskDataSize(size uint64) func(*LSMTree) {
	return func(l *LSMTree) {
		l.config.Merge.DataSize = size
	}
}

func MergeConfig(ms MergeSettings) func(*LSMTree) {
	return func(l *LSMTree) {
		l.config.Merge = ms
	}
}

func defaultMergeConfig() *Config {
	return &Config{
		MemtblDataSize: defaultMemTableThreshold,
		Merge: MergeSettings{
			MaxLevels:        255,
			Interval:         time.Duration(2) * time.Second,
			NumberOfSstFiles: 8,
			DataSize:         1 << 10 * 1 << 10, // 1MB
		},
	}
}
