package main

import (
	"database/sql"
	"encoding/base64"
	"log"

	"github.com/dustin/randbo"
	_ "github.com/lib/pq"

	pgds "github.com/whyrusleeping/pg-datastore"

	"io/ioutil"
	"os"
	"testing"

	ds "github.com/jbenet/go-datastore"
	flatfs "github.com/jbenet/go-datastore/flatfs"
	levelds "github.com/jbenet/go-datastore/leveldb"
	boltdb "github.com/whyrusleeping/bolt-datastore"
)

var blocks [][]byte
var keys []ds.Key
var src = randbo.New()

type ConstructionFunc func(tb testing.TB, path string) ds.Datastore

func runDiskBackedBench(cf ConstructionFunc, b *testing.B, tmpfs bool) {
	dir, cleanup := RandTestDir(b, tmpfs)
	defer cleanup()

	dstore := cf(b, dir)

	runBlockPutBenchmark(dstore, b)
}

func runBlockPutBenchmark(dstore ds.Datastore, b *testing.B) {
	b.ResetTimer()
	err := BlockWriteTest(b, dstore)
	if err != nil {
		b.Fatal(err)
	}
}

func getEnoughData(count int) {
	for len(blocks) < count {
		key := make([]byte, 32)
		src.Read(key)
		keystr := base64.StdEncoding.EncodeToString(key)
		block := make([]byte, DefaultBlocksize)
		src.Read(block)
		blocks = append(blocks, block)
		keys = append(keys, ds.NewKey(keystr))
	}

}

func BlockWriteTest(b *testing.B, d ds.Datastore) error {
	getEnoughData(b.N)

	b.ResetTimer()
	if b.N > 100000 {
		b.SkipNow()
	}
	for i := 0; i < b.N; i++ {
		err := d.Put(keys[i], blocks[i])
		if err != nil {
			return err
		}
	}

	return nil
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
	b.Skip("yeah")
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

func BenchmarkBoltBlockPutTmpfs(b *testing.B) {
	runDiskBackedBench(mkBoltDB, b, true)
}

func BenchmarkBoltBlockPutDisk(b *testing.B) {
	runDiskBackedBench(mkBoltDB, b, false)
}

func BenchmarkPostgres(b *testing.B) {
	b.Skip("make sure you set up your postgresql database")
	db, err := sql.Open("postgres", "postgres://postgres:mysecretpassword@172.17.0.2/ipfs?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	sds := pgds.NewSqlDatastore(db)

	runBlockPutBenchmark(sds, b)
}
