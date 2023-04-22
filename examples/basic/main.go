package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/nutsdb/nutsdb"
)

var (
	db     *nutsdb.DB
	bucket string
)

func init() {
	fileDir := "/tmp/nutsdb_example"

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
	// insert
	put()
	// read
	read()

	// delete
	delete()
	// read
	read()

	// insert
	put()
	// read
	read()

	// update
	put2()
	// read
	read()
}

func delete() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			return tx.Delete(bucket, key)
		}); err != nil {
		log.Fatal(err)
	}

	// 等价于下面的逻辑
	// 但是为什么要定义一个函数作为 Update 的入参？
	// 而且这个函数是非通用的
	// 比如对于删除操作，只能根据业务定义一个具体的删除函数或者说删除事务
	// 然后将它传给 Update

	// 但是从 Update 的视角来看，被调用时，
	// 不用管传进来的事务的具体逻辑是什么，只要传进来的事务符合变量的规范即可
	// err := db.Update(delOperation)
	// if err != nil {
	// 	log.Fatal(err)
	// }
}

func delOperation(tx *nutsdb.Tx) error {
	key := "name1"
	return tx.Delete(bucket, []byte(key))
}

func put() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			val := []byte("val1")
			return tx.Put(bucket, key, val, 0)
		}); err != nil {
		log.Fatal(err)
	}
}
func put2() {
	if err := db.Update(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			val := []byte("val2")
			return tx.Put(bucket, key, val, 0)
		}); err != nil {
		log.Fatal(err)
	}
}
func read() {
	if err := db.View(
		func(tx *nutsdb.Tx) error {
			key := []byte("name1")
			e, err := tx.Get(bucket, key)
			if err != nil {
				return err
			}
			fmt.Println("val:", string(e.Value))

			return nil
		}); err != nil {
		log.Println(err)
	}
}
