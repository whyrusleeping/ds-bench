package main

import (
	"io/ioutil"
	"os"
	"testing"

	ds "github.com/jbenet/go-datastore"
	flatfs "github.com/jbenet/go-datastore/flatfs"
	levelds "github.com/jbenet/go-datastore/leveldb"
	boltdb "github.com/whyrusleeping/bolt-datastore"
)

type ConstructionFunc func(tb testing.TB, path string) ds.Datastore

func runDiskBackedBench(cf ConstructionFunc, b *testing.B, tmpfs bool) {
	dir, cleanup := RandTestDir(b, tmpfs)
	defer cleanup()

	dstore := cf(b, dir)

	runBlockPutBenchmark(dstore, b)
}

func runBlockPutBenchmark(dstore ds.Datastore, b *testing.B) {
	b.ResetTimer()
	err := BlockWriteTest(dstore, b.N, DefaultBlocksize)
	if err != nil {
		b.Fatal(err)
	}
}

var _ = levelds.Options{}

func RandTestDir(t testing.TB, tmpfs bool) (string, func()) {
	var base string
	if !tmpfs {
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}

		base = cwd
	}

	dir, err := ioutil.TempDir(base, ".ds-test")
	if err != nil {
		t.Fatal(err)
	}

	return dir, func() { os.RemoveAll(dir) }
}

func mkBoltDB(tb testing.TB, path string) ds.Datastore {
	bds, err := boltdb.NewBoltDatastore(path, "bench")
	if err != nil {
		tb.Fatal(err)
	}

	return bds
}

func flatfsSyncFunc(sync bool) ConstructionFunc {
	return func(tb testing.TB, path string) ds.Datastore {
		flatfsds, err := flatfs.New(path, 2, sync)
		if err != nil {
			tb.Fatal(err)
		}
		return flatfsds
	}
}

func BenchmarkMapBlockPut(b *testing.B) {
	runBlockPutBenchmark(ds.NewMapDatastore(), b)
}

func BenchmarkFlatfsBlockPutTmpfs(b *testing.B) {
	runDiskBackedBench(flatfsSyncFunc(true), b, true)
}

func BenchmarkFlatfsBlockPutDisk(b *testing.B) {
	runDiskBackedBench(flatfsSyncFunc(true), b, false)
}

func BenchmarkFlatfsBlockPutTmpfsNoSync(b *testing.B) {
	runDiskBackedBench(flatfsSyncFunc(false), b, true)
}

func BenchmarkFlatfsBlockPutDiskNoSync(b *testing.B) {
	runDiskBackedBench(flatfsSyncFunc(false), b, false)
}

func BenchmarkFlatfsBlockPutTmpfsEvenBlocks(b *testing.B) {
	old := DefaultBlocksize
	DefaultBlocksize = 1024 * 256
	runDiskBackedBench(flatfsSyncFunc(false), b, true)
	DefaultBlocksize = old
}

func BenchmarkFlatfsBlockPutDiskEvenBlocks(b *testing.B) {
	old := DefaultBlocksize
	DefaultBlocksize = 1024 * 256
	runDiskBackedBench(flatfsSyncFunc(false), b, false)
	DefaultBlocksize = old
}

func BenchmarkBoltBlockPutTmpfs(b *testing.B) {
	runDiskBackedBench(mkBoltDB, b, true)
}

func BenchmarkBoltBlockPutDisk(b *testing.B) {
	runDiskBackedBench(mkBoltDB, b, false)
}
