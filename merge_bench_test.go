package nutsdb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

)

func BenchmarkMergeVariants(b *testing.B) {
	modes := []mergeTestMode{{name: "legacy", enableMergeV2: false}, {name: "mergev2", enableMergeV2: true}}
	for _, mode := range modes {
		mode := mode
		b.Run(mode.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				dir := filepath.Join(os.TempDir(), fmt.Sprintf("nutsdb-merge-bench-%s-%d", mode.name, i))
				removeDir(dir)

				opts := DefaultOptions
				opts.Dir = dir
				opts.SegmentSize = 32 * KB
				opts.EnableHintFile = true
				opts.EnableMergeV2 = mode.enableMergeV2

				db, err := Open(opts)
				if err != nil {
					b.Fatalf("open db: %v", err)
				}

				bucket := "bench"
				if err := db.Update(func(tx *Tx) error {
					return tx.NewBucket(DataStructureBTree, bucket)
				}); err != nil {
					b.Fatalf("create bucket: %v", err)
				}

				entries := 2000
				if err := db.Update(func(tx *Tx) error {
					for j := 0; j < entries; j++ {
						key := []byte(fmt.Sprintf("key-%06d", j))
						value := []byte(fmt.Sprintf("value-%06d", j))
						if err := tx.Put(bucket, key, value, Persistent); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatalf("populate data: %v", err)
				}

				if err := db.Update(func(tx *Tx) error {
					for j := 0; j < entries/2; j++ {
						key := []byte(fmt.Sprintf("key-%06d", j))
						if err := tx.Delete(bucket, key); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatalf("delete data: %v", err)
				}

				b.StartTimer()
				if err := db.Merge(); err != nil {
					b.Fatalf("merge: %v", err)
				}
				b.StopTimer()

				if err := db.Close(); err != nil {
					b.Fatalf("close db: %v", err)
				}
				removeDir(dir)
			}
		})
	}
}
