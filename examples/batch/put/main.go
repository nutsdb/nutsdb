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
	opt := nutsdb.DefaultOptions
	opt.Dir = "/tmp/nutsdbexample/example_batch"
	db, _ = nutsdb.Open(opt)
	bucket = "bucket1"
}

func main() {
	time2.Start()

	end := 1

	for j := 1; j <= end; j++ {
		if err := db.Update(
			func(tx *nutsdb.Tx) error {
				for i := (j - 1) * 1000000; i < j*1000000; i++ {
					key := []byte("namename" + strconv2.IntToStr(i))
					val := []byte("valvalvavalvalvalvavalvalvalvavalvalvalvaval" + strconv2.IntToStr(i))
					if err := tx.Put(bucket, key, val, 0); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
			log.Fatal(err)
		}

	}

	fmt.Println("batch put data cost: ", time2.End())
	// MacBook Pro (15-inch, 2017),3.1 GHz Intel Core i7, 16 GB 2133 MHz LPDDR3
	// 100w data batch put data cost:  2.066787s
}
