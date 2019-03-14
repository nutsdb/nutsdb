package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/xujiajun/nutsdb"
	"github.com/xujiajun/utils/strconv2"
)

func readWorker(id int, jobs <-chan int, results chan<- struct{}) {
	for j := range jobs {
		fmt.Println("readWorker", id, "started  job", j)
		time.Sleep(time.Second)

		key := strconv2.IntToStr(j)
		if err := db.View(
			func(tx *nutsdb.Tx) error {
				e, err := tx.Get(bucket, []byte(key))
				if err != nil {
					return err
				}
				fmt.Println("val:", string(e.Value))

				return nil
			}); err != nil {
			log.Println(err)
		}

		fmt.Println("readWorker", id, "finished job", j)
		results <- struct{}{}
	}
}

func writeWorker(id int, jobs <-chan int, results chan<- struct{}) {
	for j := range jobs {
		fmt.Println("writeWorker", id, "started  job", j)
		time.Sleep(time.Second)

		key := strconv2.IntToStr(j)
		if err := db.Update(
			func(tx *nutsdb.Tx) error {
				val := []byte("val" + key)
				return tx.Put(bucket, []byte(key), val, 0)
			}); err != nil {
			log.Fatal(err)
		}

		fmt.Println("writeWorker", id, "finished job", j)
		results <- struct{}{}
	}
}

var (
	db     *nutsdb.DB
	err    error
	bucket string
)

func removeFileDir(fileDir string) {
	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			fmt.Println(fileDir + "/" + name)
			err := os.Remove(fileDir + "/" + name)
			if err != nil {
				panic(err)
			}
		}
	}
}

func main() {
	removeFlag := false

	opt := nutsdb.DefaultOptions
	fileDir := "/tmp/nutsdb_example_concurrence"

	if removeFlag {
		removeFileDir(fileDir)
	}

	opt.Dir = fileDir
	opt.SegmentSize = 1024 * 1024 // 1MB
	db, err = nutsdb.Open(opt)
	if err != nil {
		panic(err)
	}

	bucket = "bucketForString"

	readJobs := make(chan int, 10)
	writeJobs := make(chan int, 10)
	readResults := make(chan struct{}, 10)
	writeResults := make(chan struct{}, 10)

	for w := 1; w <= 3; w++ {
		go readWorker(w, readJobs, readResults)
	}

	for w := 1; w <= 3; w++ {
		go writeWorker(w, writeJobs, writeResults)
	}

	for j := 1; j <= 10; j++ {
		readJobs <- j
	}

	for j := 1; j <= 10; j++ {
		writeJobs <- j
	}

	close(readJobs)
	close(writeJobs)

	for a := 1; a <= 10; a++ {
		<-readResults
	}
	for a := 1; a <= 10; a++ {
		<-writeResults
	}
}
