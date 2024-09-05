package main

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

func IndexNestedLoop(file_path_r string, file_path_s string, tuples int, rIndex string, sIndex string, totalValueSize int) {
	db_r, _ := openPebbleDB("R", 16<<20)
	db_s, _ := openPebbleDB("S", 16<<20)
	defer db_r.Close()
	defer db_s.Close()

	pk_s, pk_r := generateData(uint64(tuples), uint64(tuples), 0.2, 1, 1, false, 10)

	var skew float64
	var data_r, data_s []uint64
	// Read the data from the binary files
	if strings.Contains(file_path_r, "skew") {
		fmt.Sscanf(file_path_r, "skew_%f", &skew)
		data_r, data_s = generateData(uint64(tuples), uint64(tuples), 0.2, 1, skew, true, 10)
	} else {
		data_r, _ = readBinaryFile(file_path_r, tuples)
		fmt.Printf("Read %d entries from binary file\n", len(data_r))
		data_s, _ = readBinaryFile(file_path_s, tuples)
		fmt.Printf("Read %d entries from binary file\n", len(data_s))
	}

	startTime := time.Now()
	batchWriteDataToPebble(db_r, data_r, pk_r, 100000, "second", 10, 10, totalValueSize)
	duration := time.Since(startTime)
	fmt.Printf("Write %d entries to Pebble R time: %v\n", len(data_r), duration)

	index_s, _ := openPebbleDB("S_Index", 16<<20)

	startTime = time.Now()
	defer index_s.Close()
	if strings.Contains(sIndex, "Comp") {
		if strings.Contains(sIndex, "covering") {
			buildCoveringCompositeIndex(db_s, index_s, data_s, pk_s, totalValueSize, 10, 10, false, 1000)
		} else {
			_ = batchWriteDataToPebble(db_s, data_s, pk_s, 100000, "second", 10, 10, totalValueSize)
			fmt.Printf("Write %d entries to Pebble S time: %v\n", len(data_s), duration)
			buildCompositeIndex(index_s, data_s, pk_s, 10, 10, 1000)
		}
	}

	if strings.Contains(sIndex, "Lazy") || strings.Contains(sIndex, "Eager") {
		if strings.Contains(sIndex, "covering") {
			if strings.Contains(sIndex, "Lazy") {
				buildCoveringLazyIndex(db_s, index_s, data_s, pk_s, totalValueSize, 10, 10, false, 1000)
			} else {
				buildCoveringEagerIndex(db_s, index_s, data_s, pk_s, totalValueSize, 10, 10, false, 1000)
			}
		} else {
			_ = batchWriteDataToPebble(db_s, data_s, pk_s, 100000, "second", 10, 10, totalValueSize)
			fmt.Printf("Write %d entries to Pebble S time: %v\n", len(data_s), duration)
			if strings.Contains(sIndex, "Lazy") {
				buildLazyIndex(index_s, data_s, pk_s, totalValueSize, 10, 10, 1000)
			} else {
				buildEagerIndex(index_s, data_s, pk_s, totalValueSize, 10, 10, 1000)
			}
		}
	}

	index_time := time.Since(startTime)

	startTime = time.Now()

	fmt.Println("Index nested loop joining...")
	primarySize := 10
	secondarySize := 10

	readOptions := &pebble.IterOptions{}
	itR, _ := db_r.NewIter(readOptions)
	defer itR.Close()

	itS, _ := index_s.NewIter(readOptions)
	defer itS.Close()

	var matches uint64
	var dataTime, indexTime, postTime float64

	if strings.Contains(sIndex, "Eager") || strings.Contains(sIndex, "Lazy") {
		for itR.First(); itR.Valid(); itR.Next() {
			tmpR := string(itR.Value())[:secondarySize]

			start := time.Now()
			value, closer, err := index_s.Get([]byte(tmpR))
			if err == nil {
				closer.Close()
			} else {
				continue
			}

			// fmt.Println("value: ", string(value))
			indexTime += time.Since(start).Seconds()

			if strings.Contains(sIndex, "covering") {
				valueSplit := SplitStringEveryNChars(string(value), totalValueSize)
				matches += uint64(len(valueSplit))
			} else {
				valueSplit := SplitStringEveryNChars(string(value), 10)

				for x := range valueSplit {
					start := time.Now()
					// fmt.Println("x: ", x)
					value, closer, err := db_s.Get([]byte(x[:primarySize]))
					if err == nil {
						closer.Close()
					} else {
						continue
					}
					dataTime += time.Since(start).Seconds()

					if string(value)[:secondarySize] == tmpR {
						matches++
					}
				}
			}
			closer.Close()
		}
	} else if strings.Contains(sIndex, "Comp") {
		for itR.First(); itR.Valid(); itR.Next() {
			tmpR := string(itR.Value())[:secondarySize]
			secondaryKeyLower := tmpR + strings.Repeat("0", primarySize)
			secondaryKeyUpper := tmpR + strings.Repeat("9", primarySize)

			start := time.Now()
			itS.SeekGE([]byte(secondaryKeyLower))
			indexTime += time.Since(start).Seconds()

			for itS.Valid() {
				itSKey := string(itS.Key())

				if itSKey > secondaryKeyUpper {
					break
				}

				tmpS := itSKey[:secondarySize]

				if tmpS == tmpR {
					if !strings.Contains(sIndex, "covering") {
						start := time.Now()
						value, closer, err := db_s.Get([]byte(itSKey[secondarySize:]))
						if err == nil {
							closer.Close()
						} else {
							continue
						}
						dataTime += time.Since(start).Seconds()

						if string(value)[:secondarySize] == tmpS {
							matches++
						}
					} else {
						matches++
					}
				}

				start = time.Now()
				itS.Next()
				indexTime += time.Since(start).Seconds()

				if !itS.Valid() || string(itS.Key()) > secondaryKeyUpper {
					break
				}
			}
		}
	}
	join_time := time.Since(startTime)

	fmt.Printf("!!!!Indexing time: %v\n", index_time)
	fmt.Printf("!!!!Joining time: %v\n", join_time)
	fmt.Printf("!!!!Matches: %d\n", matches)
	fmt.Printf("Data retrieval time: %f\n", dataTime)
	fmt.Printf("Index retrieval time: %f\n", indexTime)
	fmt.Printf("Post-list processing time: %f\n", postTime)
}

func NestedLoop(file_path_r string, file_path_s string, tuples int, totalValueSize int) {
	fmt.Println("Joining...")

	// Reset statistics (if Pebble has a similar mechanism, it would be used here)

	dbR, _ := openPebbleDB("R", 16<<20)
	dbS, _ := openPebbleDB("S", 16<<20)
	var skew float64
	var dataR, dataS []uint64
	// Read the data from the binary files
	if strings.Contains(file_path_r, "skew") {
		fmt.Sscanf(file_path_r, "skew_%f", &skew)
		dataR, dataS = generateData(uint64(tuples), uint64(tuples), 0.2, 1, skew, true, 10)
	} else {
		dataR, _ = readBinaryFile(file_path_r, tuples)
		fmt.Printf("Read %d entries from binary file\n", len(dataR))
		dataS, _ = readBinaryFile(file_path_s, tuples)
		fmt.Printf("Read %d entries from binary file\n", len(dataS))
	}

	// Write the data to the Pebble databases
	pk_s, pk_r := generateData(uint64(tuples), uint64(tuples), 0.2, 1, 1, false, 10)
	err := batchWriteDataToPebble(dbR, dataR, pk_r, 100000, "second", 10, 10, totalValueSize)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Write %d entries to Pebble R\n", len(dataR))

	err = batchWriteDataToPebble(dbS, dataS, pk_s, 100000, "primary", 10, 10, totalValueSize)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Write %d entries to Pebble S\n", len(dataS))

	iterR, _ := dbR.NewIter(nil)
	defer iterR.Close()

	start_time := time.Now()
	matches := 0
	for iterR.First(); iterR.Valid(); iterR.Next() {
		tmpR := string(iterR.Value()[:10])
		_, closer, err := dbS.Get([]byte(tmpR))
		if err == nil {
			closer.Close()
			matches++
		}
	}

	fmt.Printf("!!!!Joining time: %v\n", time.Since(start_time))
	fmt.Printf("!!!!Matches: %d\n", matches)
	dbR.Close()
	dbS.Close()
}
