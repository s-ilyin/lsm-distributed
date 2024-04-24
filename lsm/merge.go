package lsm

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math"
	"os"
	"time"

	"github.com/s-ilyin/lsm-distributed/lsm/sst"
)

// MergeJob runs as a background thread and coordinates when to check SST levels for merging.
func (t *LSMTree) mergeJob() {
	defer t.wg.Done()
	if t.config.Merge.Interval == 0 {
		log.Println("mergeJob interval not set, stopping goroutine")
		return
	}
	ticker := time.NewTicker(t.config.Merge.Interval)

	for {
		select {
		case <-ticker.C:
			//log.Println("LSM merge job woke up")
			if err := t.merge(); err != nil {
				t.logger.Debug(err.Error())
			}
		case <-t.ctx.Done():
			return
		}

	}
}

func (s *LSMTree) SetMergeSettings(ms MergeSettings) {
	s.config.Merge = ms
}

func (t *LSMTree) merge() error {

	for lvl := sst.Level(0); lvl < t.fobserver.Levels(); lvl++ {
		var (
			isMerge = false

			num = t.config.Merge.NumberOfSstFiles
		)

		l := t.fobserver.Len(lvl)

		// if t.debug {
		// 	m := num > 0 && len(files) >= num
		// 	t.logger.Debug("debug", slog.Int("num", num), slog.Int("files", len(files)))
		// 	t.logger.Debug("нужно сливать?", slog.Bool("is_merge", m))
		// }
		//log.Println(lvl, len(files), t.config.Merge.DataSize, t.fobserver.Size(lvl), num > 0 && len(files) >= num && int64(t.config.Merge.DataSize) >= t.fobserver.Size(lvl))
		if num > 0 && l >= num && l > t.config.Merge.NumberOfSstFiles*(int(lvl)+1) && t.fobserver.Size(lvl) >= int64(t.config.Merge.DataSize) {
			//log.Printf("merge level %d, number of files %d exceeded merge threshold", lvl, len(files))
			isMerge = true

			if sst.Level(lvl) == t.config.Merge.MaxLevels {
				// условия для последнего уровня
				isMerge = false
			}
		}

		if isMerge {
			//t.logger.Debug("debug", slog.Int("merge", lvl))
			if err := t.compact(lvl); err != nil {
				t.logger.Error(err.Error())
			}
		}
	}

	return nil
}

// Merge берет все текущие SST-файлы на уровне и объединяет их с
// SST-файлами на следующем уровне дерева LSM. Во время этого
// процесса данные уплотняются, и все старые значения ключей или надгробные плиты удаляются безвозвратно.
func (t *LSMTree) compact(level sst.Level) error {
	// Общий алгоритм
	//
	// - найти путь к уровню, получить все sst-файлы
	// - найти путь для уровня+1, получить все sst-файлы
	// - загружаем содержимое файлов в кучу (в будущем: передаем их потоком)
	// - выписать файлы обратно в новый временный каталог
	// - получить блокировку дерева
	// - поменять местами уровень+1 с новым каталогом
	// - удалить все старые файлы
	// - очистить все данные в памяти для файлов
	// - снять блокировки, слияние завершено
	// - записываем в syslog, считаем WAL
	// TODO: если level == tree.merge.MaxLevels, то уплотнить этот уровень вместо слияния в l+1

	nextLevel := level + 1
	currentMaxLvl := t.fobserver.MaxLevel()
	if level > currentMaxLvl {
		desc := fmt.Sprintf("merge cannot process level %d because the tree only has %d levels", level, currentMaxLvl)
		log.Println(desc)

		return errors.New(desc)
	}

	if level > 0 && level == sst.Level(t.config.Merge.MaxLevels) {
		// if max lvl

		return nil
	}
	if !t.config.Merge.Immediate {
		t.lock.Lock()
		defer t.lock.Unlock()
	}
	currFilesLevel := t.fobserver.Level(level)[:]
	nextFilesLevel := t.fobserver.Level(nextLevel)[:]
	mergeFilesLevels := append(currFilesLevel, nextFilesLevel...)

	nextLvlPath := sst.PathForLevel(t.root, nextLevel)

	var rm bool
	if level == currentMaxLvl {
		rm = true
	}

	readers := make([]*sst.Reader, len(mergeFilesLevels))
	for idx := range mergeFilesLevels {
		readers[idx] = mergeFilesLevels[idx].Reader
	}

	size := int64(t.config.MemtblDataSize * uint32(math.Pow(2, float64(level+1))))
	sparseKeyDistance := t.sparseKeyDistance * int32(math.Pow(2, float64(level+1)))

	//t.logger.Debug("debug", slog.Int("readers", len(readers)))
	mergedir, err := sst.Compact(t.root, readers, size, sparseKeyDistance, rm)
	if err != nil {
		return err
	}

	if err := os.RemoveAll(nextLvlPath); err != nil {
		return err
	}
	if err := os.Rename(mergedir, nextLvlPath); err != nil {
		return err
	}
	for idx := range currFilesLevel {
		os.Remove(currFilesLevel[idx].Reader.Name())
	}

	//log.Println("reload lvl")
	if err := t.fobserver.Reload(nextLevel); err != nil {
		return err
	}
	t.fobserver.Flush(level)

	if t.debug {
		t.logger.Debug("уплотнение закончено", slog.Int("lvl", int(level)))
	}

	return nil
}
