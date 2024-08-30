package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

func SplitStringEveryNChars(value string, chunkSize int) map[string]struct{} {
	chunks := make(map[string]struct{})
	length := len(value)

	for i := 0; i < length; i += chunkSize {
		end := i + chunkSize
		if end > length {
			end = length
		}
		chunk := value[i:end]
		chunks[chunk] = struct{}{} // Add the chunk to the set
	}

	return chunks
}

func buildCompositeIndex(index *pebble.DB, data []uint64, pk []uint64, secondarySize int, primarySize int, batchSize int) {
	fmt.Println("Building composite index with batch writes...")
	usePk := len(pk) != 0
	indexTime := 0.0

	indexBatch := index.NewBatch()

	for i := 0; i < len(data); i++ {
		if (i+1)%5000000 == 0 {
			fmt.Printf("%d million\n", (i+1)/1000000)
		}

		// 构建 tmpSecondary 和 tmpPrimary
		tmpSecondary := fmt.Sprintf("%d", data[i])
		tmpPrimary := fmt.Sprintf("%d", i)
		if usePk {
			tmpPrimary = fmt.Sprintf("%010d", pk[i])
		}

		// 按照 buildCoveringCompositeIndex 的方式构建 secondaryKey
		secondaryKey := fmt.Sprintf("%0*s%s%0*s%s",
			secondarySize-min(secondarySize, len(tmpSecondary)), "",
			tmpSecondary,
			primarySize-min(primarySize, len(tmpPrimary)), "",
			tmpPrimary)
		// fmt.Println("secondaryKey: ", secondaryKey)
		// 批量插入到索引中
		timer := time.Now()
		indexBatch.Set([]byte(secondaryKey), nil, nil)
		// fmt.Println("secondaryKey: ", secondaryKey)
		indexTime += time.Since(timer).Seconds()

		// 批量提交
		if (i+1)%batchSize == 0 {
			err := indexBatch.Commit(pebble.Sync)
			if err != nil {
				log.Fatalf("Failed to commit index batch: %v", err)
			}
			indexBatch.Reset()
		}
	}

	// 提交剩余的数据
	if indexBatch.Count() > 0 {
		err := indexBatch.Commit(pebble.Sync)
		if err != nil {
			log.Fatalf("Failed to commit remaining index batch: %v", err)
		}
	}

	fmt.Printf("Index build time: %f seconds\n", indexTime)
	indexBatch.Close()
}

func buildCoveringCompositeIndex(db *pebble.DB, index *pebble.DB, data []uint64, pk []uint64, totalValueSize int, secondarySize int, primarySize int, nonCovering bool, batchSize int) {
	fmt.Println("Building covering composite index with batch writes...")
	usePk := len(pk) != 0
	dbBatch := db.NewBatch()
	indexBatch := index.NewBatch()
	dataTime, indexTime := 0.0, 0.0
	deletes := 0
	inserts := 0
	for i := 0; i < len(data); i++ {
		if (i+1)%5000000 == 0 {
			fmt.Printf("%d million\n", (i+1)/1000000)
		}

		tmpKey := fmt.Sprintf("%0*s%d",
			primarySize-min(primarySize, len(fmt.Sprintf("%d", i))), "",
			i)
		if usePk {
			tmpKey = fmt.Sprintf("%010d", pk[i])
		}

		tmpValue := fmt.Sprintf("%0*s%d%0*s",
			secondarySize-min(secondarySize, len(fmt.Sprintf("%d", data[i]))), "",
			data[i],
			totalValueSize-secondarySize, "")

		timer1 := time.Now()
		tmpSecondary, closer, err := db.Get([]byte(tmpKey))

		dataTime += time.Since(timer1).Seconds()

		timer2 := time.Now()
		if err == nil && len(tmpSecondary) > 0 {
			deletes++
			// fmt.Println("delete: ", string([]byte(fmt.Sprintf("%0*s%s",
			// 	secondarySize, tmpSecondary, tmpKey))))
			// fmt.Println(string(tmpSecondary))
			indexBatch.Delete([]byte(fmt.Sprintf("%0*s%s",
				secondarySize, tmpSecondary[:secondarySize], tmpKey)), nil)
		}

		if nonCovering {
			indexBatch.Set([]byte(fmt.Sprintf("%0*s%s",
				secondarySize, tmpValue[:secondarySize], tmpKey)), nil, nil)
		} else {
			// fmt.Println("insert: ", string([]byte(fmt.Sprintf("%0*s%s",
			// 	secondarySize, tmpValue[:secondarySize], tmpKey))))
			indexBatch.Set([]byte(fmt.Sprintf("%0*s%s",
				secondarySize, tmpValue[:secondarySize], tmpKey)),
				[]byte(tmpValue[secondarySize:]), nil)
			inserts++
		}
		indexTime += time.Since(timer2).Seconds()

		dbBatch.Set([]byte(tmpKey), []byte(tmpValue), nil)
		if err == nil {
			closer.Close()
		}
		if (i+1)%batchSize == 0 {
			err = dbBatch.Commit(pebble.Sync)
			if err != nil {
				log.Fatalf("Failed to commit db batch: %v", err)
			}
			dbBatch.Reset()

			timer4 := time.Now()
			err = indexBatch.Commit(pebble.Sync)
			if err != nil {
				log.Fatalf("Failed to commit index batch: %v", err)
			}
			indexBatch.Reset()
			indexTime += time.Since(timer4).Seconds()
		}

	}

	if dbBatch.Count() > 0 {
		err := dbBatch.Commit(pebble.Sync)
		if err != nil {
			log.Fatalf("Failed to commit remaining db batch: %v", err)
		}
	}
	if indexBatch.Count() > 0 {
		err := indexBatch.Commit(pebble.Sync)
		if err != nil {
			log.Fatalf("Failed to commit remaining index batch: %v", err)
		}
	}
	fmt.Println("getDataTime: ", dataTime, "buildIndexTime: ", indexTime)
	fmt.Println("deletes: ", deletes, "inserts: ", inserts)
	dbBatch.Close()
	indexBatch.Close()

}

func buildLazyIndex(index *pebble.DB, data []uint64, pk []uint64, totalValueSize int, secondarySize int, primarySize int, batchSize int) {
	fmt.Println("Building lazy index with batch writes...")
	usePk := len(pk) != 0
	indexBatch := index.NewBatch()
	indexTime := 0.0

	for i := 0; i < len(data); i++ {
		if (i+1)%5000000 == 0 {
			fmt.Printf("%d million\n", (i+1)/1000000)
		}

		tmpSecondary := fmt.Sprintf("%d", data[i])
		tmpPrimary := fmt.Sprintf("%d", i)
		if usePk {
			tmpPrimary = fmt.Sprintf("%010d", pk[i])
		}

		secondaryKey := fmt.Sprintf("%0*s%s",
			secondarySize-min(secondarySize, len(tmpSecondary)), "",
			tmpSecondary)

		value := fmt.Sprintf("%0*s%s",
			primarySize-min(primarySize, len(tmpPrimary)), "",
			tmpPrimary)

		timer := time.Now()
		indexBatch.Merge([]byte(secondaryKey), []byte(value), nil)
		indexTime += time.Since(timer).Seconds()

		if (i+1)%batchSize == 0 {
			err := indexBatch.Commit(pebble.Sync)
			if err != nil {
				log.Fatalf("Failed to commit index batch: %v", err)
			}
			indexBatch.Reset()
		}
	}

	if indexBatch.Count() > 0 {
		err := indexBatch.Commit(pebble.Sync)
		if err != nil {
			log.Fatalf("Failed to commit remaining index batch: %v", err)
		}
	}

	fmt.Printf("Lazy index build time: %f seconds\n", indexTime)
	indexBatch.Close()
}

func buildCoveringLazyIndex(db *pebble.DB, index *pebble.DB, data []uint64, pk []uint64, totalValueSize int, secondarySize int, primarySize int, nonCovering bool, batchSize int) {
	fmt.Println("Building covering lazy index with batch writes...")
	usePk := len(pk) != 0
	dbBatch := db.NewBatch()
	indexBatch := index.NewBatch()
	dataTime, indexTime, postListTime := 0.0, 0.0, 0.0

	for i := 0; i < len(data); i++ {
		if (i+1)%5000000 == 0 {
			fmt.Printf("%d million\n", (i+1)/1000000)
		}

		tmpPrimary := fmt.Sprintf("%0*s%d",
			primarySize-min(primarySize, len(fmt.Sprintf("%d", i))), "",
			i)
		if usePk {
			tmpPrimary = fmt.Sprintf("%010d", pk[i])
		}

		tmpSecondary := fmt.Sprintf("%0*s%d%0*s",
			secondarySize-min(secondarySize, len(fmt.Sprintf("%d", data[i]))), "",
			data[i],
			totalValueSize-secondarySize, "")

		timer1 := time.Now()
		previousSecondary, closer1, err := db.Get([]byte(tmpPrimary))
		if err == nil {
			defer closer1.Close()
		}
		dataTime += time.Since(timer1).Seconds()

		if err == nil {
			timer2 := time.Now()
			oldValue, closer2, err := index.Get([]byte(previousSecondary[:secondarySize]))
			if err == nil && len(oldValue) > 0 {
				closer2.Close()
			}
			indexTime += time.Since(timer2).Seconds()

			if err == nil {
				timer3 := time.Now()
				valueSplit := SplitStringEveryNChars(string(oldValue), totalValueSize)
				// for i, chunk := range valueSplit {
				// 	fmt.Printf("Chunk %d: %s\n", i+1, chunk)
				// }
				// fmt.Println("valueSplit: ", len(valueSplit))
				newValueSplit := []string{}
				for v := range valueSplit {
					if v[:primarySize] != tmpPrimary {
						newValueSplit = append(newValueSplit, v)
					}
				}
				postListTime += time.Since(timer3).Seconds()
				// fmt.Println("newValueSplit: ", len(newValueSplit))
				if len(newValueSplit) == 0 {
					timer4 := time.Now()
					indexBatch.Delete([]byte(previousSecondary[:secondarySize]), nil)
					indexTime += time.Since(timer4).Seconds()
				} else {
					timer5 := time.Now()
					newValue := strings.Join(newValueSplit, "")
					indexBatch.Set([]byte(previousSecondary[:secondarySize]), []byte(newValue), nil)
					indexTime += time.Since(timer5).Seconds()
				}
			}
		}

		timer6 := time.Now()
		if nonCovering {
			indexBatch.Merge([]byte(tmpSecondary[:secondarySize]), []byte(tmpPrimary), nil)
		} else {
			indexBatch.Merge([]byte(tmpSecondary[:secondarySize]), []byte(tmpPrimary+strings.Repeat("0", totalValueSize-secondarySize)), nil)
		}
		indexTime += time.Since(timer6).Seconds()
		// fmt.Println("tmpPrimary: ", tmpPrimary, "tmpSecondary: ", tmpSecondary)
		dbBatch.Set([]byte(tmpPrimary), []byte(tmpSecondary), nil)
		// closer1.Close()
		if (i+1)%batchSize == 0 {
			err = dbBatch.Commit(pebble.Sync)
			if err != nil {
				log.Fatalf("Failed to commit db batch: %v", err)
			}
			dbBatch.Reset()

			err = indexBatch.Commit(pebble.Sync)
			if err != nil {
				log.Fatalf("Failed to commit index batch: %v", err)
			}
			indexBatch.Reset()
		}
		// closer1.Close()
	}

	if dbBatch.Count() > 0 {
		err := dbBatch.Commit(pebble.Sync)
		if err != nil {
			log.Fatalf("Failed to commit remaining db batch: %v", err)
		}
	}
	if indexBatch.Count() > 0 {
		err := indexBatch.Commit(pebble.Sync)
		if err != nil {
			log.Fatalf("Failed to commit remaining index batch: %v", err)
		}
	}

	fmt.Printf("Covering lazy index build time: %f seconds\n", indexTime)
	fmt.Printf("Post-list processing time: %f seconds\n", postListTime)
	dbBatch.Close()
	indexBatch.Close()
}

func buildEagerIndex(index *pebble.DB, data []uint64, pk []uint64, totalValueSize int, secondarySize int, primarySize int, batchSize int) {
	fmt.Println("Building eager index with batch writes...")
	usePk := len(pk) != 0
	updateTime, eagerTime := 0.0, 0.0

	indexBatch := index.NewBatch()
	defer indexBatch.Close()

	pendingBatch := make(map[string]string)
	for i := 0; i < len(data); i++ {
		if (i+1)%5000000 == 0 {
			fmt.Printf("%d million\n", (i+1)/1000000)
		}

		tmpSecondary := fmt.Sprintf("%0*s%d", secondarySize-min(secondarySize, len(fmt.Sprintf("%d", data[i]))), "", data[i])
		tmpPrimary := fmt.Sprintf("%0*s%d", primarySize-min(primarySize, len(fmt.Sprintf("%d", i))), "", i)
		if usePk {
			tmpPrimary = fmt.Sprintf("%010d", pk[i])
		}
		timer1 := time.Now()

		tmpPrimaryVal, closer, err := index.Get([]byte(tmpSecondary))
		if err == nil {
			closer.Close()
		}
		eagerTime += time.Since(timer1).Seconds()

		timer2 := time.Now()
		if err == nil {
			// Concatenate the new primary key with the existing value
			newValue := tmpPrimary + string(tmpPrimaryVal)
			if existingValue, found := pendingBatch[tmpSecondary]; found {
				// If an entry exists, concatenate the new primary key with the existing value
				newValue = tmpPrimary + existingValue
			}
			indexBatch.Set([]byte(tmpSecondary), []byte(newValue), nil)
			pendingBatch[tmpSecondary] = newValue
		} else {
			if existingValue, found := pendingBatch[tmpSecondary]; found {
				// If an entry exists, concatenate the new primary key with the existing value
				newValue := tmpPrimary + existingValue
				indexBatch.Set([]byte(tmpSecondary), []byte(newValue), nil)
				// Update the in-memory map
				pendingBatch[tmpSecondary] = newValue
			} else {
				indexBatch.Set([]byte(tmpSecondary), []byte(tmpPrimary), nil)
				pendingBatch[tmpSecondary] = tmpPrimary
			}
		}
		updateTime += time.Since(timer2).Seconds()

		if (i+1)%batchSize == 0 {
			err := indexBatch.Commit(pebble.Sync)
			if err != nil {
				log.Fatalf("Failed to commit index batch: %v", err)
			}
			indexBatch.Reset()
			pendingBatch = make(map[string]string)
		}
	}

	if indexBatch.Count() > 0 {
		err := indexBatch.Commit(pebble.Sync)
		if err != nil {
			log.Fatalf("Failed to commit remaining index batch: %v", err)
		}
	}

	// its, _ := index.NewIter(nil)
	// defer its.Close()
	// count := 0
	// for its.First(); its.Valid(); its.Next() {
	// 	valueSet := SplitStringEveryNChars(string(its.Value()), 10)
	// 	count += len(valueSet)
	// }
	// fmt.Println("!!!index count: ", count)

	fmt.Printf("Eager index update time: %f seconds\n", updateTime)
	fmt.Printf("Eager index build time: %f seconds\n", eagerTime)
}

func buildCoveringEagerIndex(db *pebble.DB, index *pebble.DB, data []uint64, pk []uint64, totalValueSize int, secondarySize int, primarySize int, nonCovering bool, batchSize int) {
	fmt.Println("Building covering eager index with batch writes...")
	usePk := len(pk) != 0
	rest := strings.Repeat("0", totalValueSize-secondarySize)

	indexBatch := index.NewBatch()
	defer indexBatch.Close()

	dbBatch := db.NewBatch()
	defer dbBatch.Close()

	pendingBatch := make(map[string]string)
	pendingBatchDB := make(map[string]string)

	for i := 0; i < len(data); i++ {
		if (i+1)%5000000 == 0 {
			fmt.Printf("%d million\n", (i+1)/1000000)
		}

		tmpPrimary := fmt.Sprintf("%0*s%d", primarySize-min(primarySize, len(fmt.Sprintf("%d", i))), "", i)
		if usePk {
			tmpPrimary = fmt.Sprintf("%010d", pk[i])
		}

		tmpSecondary := fmt.Sprintf("%0*s%d%s", secondarySize-min(secondarySize, len(fmt.Sprintf("%d", data[i]))), "", data[i], rest)

		// Step 1: Retrieve and delete old primary key references if necessary
		previousSecondary, closer, err := db.Get([]byte(tmpPrimary))
		if err == nil {
			closer.Close()
		}
		previousSecondarystr := string(previousSecondary)
		if existingValueInBatch, found := pendingBatchDB[tmpPrimary]; found {
			previousSecondarystr += existingValueInBatch
		}
		if len(previousSecondarystr) > 0 {
			oldValue, closer2, err := index.Get([]byte(previousSecondarystr[:secondarySize]))
			if err == nil {
				closer2.Close()
			}
			oldValuestr := string(oldValue)
			if existingValueInBatch, found := pendingBatch[previousSecondarystr]; found {
				oldValuestr += existingValueInBatch
			}
			if len(oldValuestr) > 0 {
				valueSplit := SplitStringEveryNChars(oldValuestr, totalValueSize)
				newValueSplit := []string{}
				for v := range valueSplit {
					if v[:primarySize] != tmpPrimary {
						newValueSplit = append(newValueSplit, v)
					}
				}

				if len(newValueSplit) == 0 {
					indexBatch.Delete([]byte(previousSecondarystr[:secondarySize]), nil)
					pendingBatch[previousSecondarystr] = ""
				} else {
					newValue := strings.Join(newValueSplit, "")
					indexBatch.Set([]byte(previousSecondarystr[:secondarySize]), []byte(newValue), nil)
					pendingBatch[previousSecondarystr[:secondarySize]] = newValue
				}
			}
		}

		// Step 2: Add or update the current entry in the index
		existingValue := ""
		if existingValueInBatch, found := pendingBatch[tmpSecondary[:secondarySize]]; found {
			existingValue = existingValueInBatch
		} else if tmpPrimaryVal, closer, err := index.Get([]byte(tmpSecondary[:secondarySize])); err == nil && len(tmpPrimaryVal) > 0 {
			defer closer.Close()
			existingValue = string(tmpPrimaryVal)
		}

		var newValue string
		if nonCovering {
			newValue = tmpPrimary + existingValue
		} else {
			newValue = tmpPrimary + rest + existingValue
		}

		indexBatch.Set([]byte(tmpSecondary[:secondarySize]), []byte(newValue), nil)
		pendingBatch[tmpSecondary[:secondarySize]] = newValue

		// Step 3: Insert the new key-value pair into the primary database
		dbBatch.Set([]byte(tmpPrimary), []byte(tmpSecondary), nil)
		pendingBatchDB[tmpPrimary] = tmpSecondary

		// Commit batches if necessary
		if (i+1)%batchSize == 0 {
			err := indexBatch.Commit(pebble.Sync)
			if err != nil {
				log.Fatalf("Failed to commit index batch: %v", err)
			}
			indexBatch.Reset()
			pendingBatch = make(map[string]string)

			err = dbBatch.Commit(pebble.Sync)
			if err != nil {
				log.Fatalf("Failed to commit db batch: %v", err)
			}
			dbBatch.Reset()
		}
	}

	// Final batch commit
	if indexBatch.Count() > 0 {
		err := indexBatch.Commit(pebble.Sync)
		if err != nil {
			log.Fatalf("Failed to commit remaining index batch: %v", err)
		}
	}

	if dbBatch.Count() > 0 {
		err := dbBatch.Commit(pebble.Sync)
		if err != nil {
			log.Fatalf("Failed to commit remaining db batch: %v", err)
		}
	}

	// Debugging: count the number of entries in the index
	its, _ := index.NewIter(nil)
	defer its.Close()
	count := 0
	for its.First(); its.Valid(); its.Next() {
		valueSet := SplitStringEveryNChars(string(its.Value()), totalValueSize)
		count += len(valueSet)
	}
	fmt.Println("!!!index count: ", count)
}
