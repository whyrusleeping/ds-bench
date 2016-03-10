package main

import (
	"encoding/base64"
	"fmt"
	"os"

	randbo "github.com/dustin/randbo"
	ds "github.com/jbenet/go-datastore"
)

var DefaultBlocksize = (1024 * 256) + 7

func BlockWriteTest(d ds.Datastore, count int, size int) error {
	src := randbo.New()
	block := make([]byte, size)
	key := make([]byte, 32)
	for i := 0; i < count; i++ {
		src.Read(block)

		keystr := base64.StdEncoding.EncodeToString(key)
		dsk := ds.NewKey(keystr)

		err := d.Put(dsk, block)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	mapds := ds.NewMapDatastore()

	err := BlockWriteTest(mapds, 1000, DefaultBlocksize)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
