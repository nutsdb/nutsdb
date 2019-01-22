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
	opt.Dir = "testdata/example_batch"
	db, _ = nutsdb.Open(opt)
	bucket = "bucket1"
}

func main() {

	time2.Start()

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			for i := 0; i < 1000000; i++ {
				key := []byte("namename" + strconv2.IntToStr(i))
				//val:=[]byte("valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvllvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalval"+strconv2.IntToStr(i))
				val := []byte("valvalvavalvalvalvavalvalvalvavalvalvalvaval" + strconv2.IntToStr(i))
				if err := tx.Put(bucket, key, val, 0); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("batch put cost: ", time2.End())
}
