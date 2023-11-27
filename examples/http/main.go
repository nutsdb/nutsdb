package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/nutsdb/nutsdb"
	"github.com/xujiajun/gorouter"
)

var (
	db     *nutsdb.DB
	bucket string
)

func init() {
	fileDir := "/tmp/nutsdb_http_example"

	files, _ := ioutil.ReadDir(fileDir)
	for _, f := range files {
		name := f.Name()
		if name != "" {
			fmt.Println(fileDir + "/" + name)
			err := os.RemoveAll(fileDir + "/" + name)
			if err != nil {
				panic(err)
			}
		}
	}
	db, _ = nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(fileDir),
		nutsdb.WithSegmentSize(1024*1024), // 1MB
	)
	bucket = "bucketForString"
}

func main() {
	mux := gorouter.New()
	mux.GET("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello, nutsdb!"))
	})

	// For example you can visit like this: http://127.0.0.1:8181/test/put/key1/value1
	mux.GET("/test/put/:key/:value", func(w http.ResponseWriter, r *http.Request) {
		key := gorouter.GetParam(r, "key")
		value := gorouter.GetParam(r, "value")
		if err := db.Update(
			func(tx *nutsdb.Tx) error {
				key := []byte(key)
				val := []byte(value)
				return tx.Put(bucket, key, val, 0)
			}); err != nil {
			log.Fatal(err)
		}
		w.Write([]byte("puts data ok!"))
	})

	// For example you can visit like this: http://127.0.0.1:8181/test/get/key1
	mux.GET("/test/get/:key", func(w http.ResponseWriter, r *http.Request) {
		key := gorouter.GetParam(r, "key")
		if err := db.View(
			func(tx *nutsdb.Tx) error {
				value, err := tx.Get(bucket, []byte(key))
				if err != nil {
					return err
				}
				fmt.Println("read data val:", string(value))
				return nil
			}); err != nil {
			log.Fatal(err)
		}

		w.Write([]byte("read data ok!"))
	})

	// run http server
	log.Fatal(http.ListenAndServe(":8181", mux))
}
