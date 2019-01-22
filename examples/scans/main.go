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
	opt.Dir = "testdata/example_scan"
	db, _ = nutsdb.Open(opt)
	bucket = "bucket1"
}

func main() {
	// initData()
	// range scans
	rangeScanDemo()

	// prefix scans
	prefixScanDemo()
}

func initData() {
	time2.Start()

	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			for i := 1; i <= 1000000; i++ {
				key := []byte("user_" + fmt.Sprintf("%07d", i))
				//val:=[]byte("valvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvllvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalvalval"+strconv2.IntToStr(i))
				val := []byte("uservalvalvavalvalvalvavalvalvalvavalvalvalvaval" + strconv2.IntToStr(i))
				if err := tx.Put(bucket, key, val, 0); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			for i := 1; i <= 1000000; i++ {
				key := []byte("key" + fmt.Sprintf("%07d", i))
				val := []byte("keyvalvalvavalvalvalvavalvalvalvavalvalvalvaval" + strconv2.IntToStr(i))
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

func rangeScanDemo() {
	time2.Start()
	if err := db.View(
		func(tx *nutsdb.Tx) error {

			start := []byte("user_0010001")
			end := []byte("user_0010010")
			if entries, err := tx.RangeScan(bucket, start, end); err != nil {
				return err
			} else {
				keys, es := nutsdb.SortedEntryKeys(entries)

				for _, key := range keys {
					fmt.Println(key, string(es[key].Value))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("range scans cost", time2.End())
}

func prefixScanDemo() {
	time2.Start()
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			//prefix := []byte("user_")
			prefix := []byte("user_000001")
			//prefix := []byte("user_0000011")
			if entries, err := tx.PrefixScan(bucket, prefix, 100); err != nil {
				return err
			} else {
				keys, es := nutsdb.SortedEntryKeys(entries)
				for _, key := range keys {
					fmt.Println(key, string(es[key].Value))
				}
			}
			return nil
		}); err != nil {
		log.Fatal(err)
	}

	fmt.Println("prefix scans cost", time2.End())
}
