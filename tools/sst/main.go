package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"

	"github.com/go-faker/faker/v4"
	"github.com/google/uuid"
	"github.com/s-ilyin/lsm-distributed/lsm/memtable"
	"github.com/s-ilyin/lsm-distributed/lsm/sst"
)

var rootDir string = "bench-tmp"
var pathtoDir = path.Join("..", "..", "lsm", "sst", rootDir)

func main() {
	if _, err := os.Stat(pathtoDir); os.IsNotExist(err) {
		os.Mkdir(pathtoDir, os.FileMode(0777))
	}
	sparseKeyDistantions := []struct {
		dist int32
		file int
	}{
		{
			2048, 0,
		},
		{
			4096, 1,
		},
		{
			8192, 2,
		},
		{
			16384, 3,
		},
	}

	for idx := range sparseKeyDistantions {
		wr, err := sst.NewWriter(path.Join(pathtoDir, fmt.Sprintf("data_000%d.sst", idx)), sst.SparseKeyDistance(sparseKeyDistantions[idx].dist))
		if err != nil {
			panic(err)
		}
		mem := memtable.NewMem()
		var limit float64
		var maxLimit = float64(1 * (1 << 10 * 1 << 10 * 1 << 10)) // 1Gi
		for limit < maxLimit {

			key := []byte(uuid.NewString())
			val := []byte(faker.Word() + faker.Word() + faker.Word() + faker.Word() + faker.Word() + faker.Word())
			mem.Put(key[:], val)

			limit += float64(len(key) + len(val) + (2 * binary.MaxVarintLen64))
		}

		it := mem.Iterator()
		for it.HasNext() {
			if err := wr.Write(it.Next()); err != nil {
				panic(err)
			}
		}
		mem.Clear()

		if err := wr.AddIdxBlock(10); err != nil {
			panic(err)
		}
		if err := wr.Close(); err != nil {
			panic(err)
		}
	}
}
