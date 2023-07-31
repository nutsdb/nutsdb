package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nutsdb/nutsdb"
	"github.com/xujiajun/utils/strconv2"
	"github.com/xujiajun/utils/time2"
)

var (
	db     *nutsdb.DB
	bucket string
)

func init() {
	db, _ = nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir("/tmp/nutsdbexample/example_batch"),
		// nutsdb.WithRWMode(nutsdb.MMap),
		// nutsdb.WithSyncEnable(false),
	)
	bucket = "bucket1"
}

func main() {
	time2.Start()

	end := 1
	t := time.Now()
	for j := 1; j <= end; j++ {
		if err := db.Update(
			func(tx *nutsdb.Tx) error {
				for i := (j - 1) * 10000; i < j*10000000; i++ {
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

	fmt.Printf("Time taken via Transaction: %v\n", time.Since(t))
	t = time.Now()
	fmt.Println("batch put data cost: ", time2.End())

	time2.Start()
	wb, err := db.NewWriteBatch()
	if err != nil {
		fmt.Println("NewWriteBatch() fail, err=", err)
	}

	for i := 0; i < 10000000; i++ {
		key := []byte("namename" + strconv2.IntToStr(i))
		val := []byte("valvalvavalvalvalvavalvalvalvavalvalvalvaval" + strconv2.IntToStr(i))
		if err := wb.Put(bucket, key, val, 0); err != nil {
			fmt.Println("batch write entry fail, err=", err)
			log.Fatal(err)
		}
	}

	if err := wb.Flush(); err != nil {
		fmt.Println("batch write flush fail, err=", err)
	}

	fmt.Printf("Time taken via write batch: %v\n", time2.End())
	fmt.Printf("Time taken via write batch11: %v\n", time.Since(t))
}
