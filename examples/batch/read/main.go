package main

import (
	"fmt"
	"log"

	"github.com/xujiajun/nutsdb"
	"github.com/xujiajun/utils/strconv2"
	"github.com/xujiajun/utils/time2"
)

var (
	db     *nutsdb.DB
	bucket string
)

func init() {
	time2.Start()
	opt := nutsdb.DefaultOptions
	opt.Dir = "/tmp/nutsdbexample/example_batch"
	db, _ = nutsdb.Open(opt)
	bucket = "bucket1"
	fmt.Println("load cost:", time2.End())
}

func main() {
	time2.Start()

	if err := db.View(
		func(tx *nutsdb.Tx) error {
			for i := 0; i < 1000000; i++ {
				key := []byte("namename" + strconv2.IntToStr(i))
				if _, err := tx.Get(bucket, key); err != nil {
					log.Println("key", string(key))
					panic(err)
					return err
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("read cost", time2.End())
}
