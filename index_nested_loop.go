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

	pk_s, pk_r := generateData(uint64(tuples), uint64(tuples), 0.2, 4, 4, false, 10)
	data_r, err := readBinaryFile(file_path_r, tuples)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Read %d entries from binary file\n", len(data_r))

	startTime := time.Now()
	err = batchWriteDataToPebble(db_r, data_r, pk_r, 100000, "second", 10, 10, totalValueSize)
	if err != nil {
		log.Fatal(err)
	}
	duration := time.Since(startTime)
	fmt.Printf("Write %d entries to Pebble R time: %v\n", len(data_r), duration)

	data_s, err := readBinaryFile(file_path_s, tuples)
	if err != nil {
		log.Fatal(err)
	}

	index_s, _ := openPebbleDB("S_Index", 16<<20)
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
			_ = batchWriteDataToPebble(db_s, data_s, pk_s, 100000, "second", 10, 10, 50)
			fmt.Printf("Write %d entries to Pebble S time: %v\n", len(data_s), duration)
			if strings.Contains(sIndex, "Lazy") {
				buildLazyIndex(index_s, data_s, pk_s, totalValueSize, 10, 10, 1000)
			} else {
				buildEagerIndex(index_s, data_s, pk_s, totalValueSize, 10, 10, 1000)
			}
		}
	}

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
	iter, _ := index_s.NewIter(nil)
	count := 0
	chunksize := 0
	if strings.Contains(sIndex, "covering") {
		chunksize = totalValueSize
	} else {
		chunksize = 10
	}
	for iter.First(); iter.Valid(); iter.Next() {
		// fmt.Println(string(iter.Key()))
		valueSplit := SplitStringEveryNChars(string(iter.Value()), chunksize)
		count += len(valueSplit)
	}
	iter.Close()
	fmt.Printf("Index S contains %d entries\n", count)
	if strings.Contains(sIndex, "Eager") || strings.Contains(sIndex, "Lazy") {
		for itR.First(); itR.Valid(); itR.Next() {
			tmpR := string(itR.Value())[:secondarySize]

			start := time.Now()
			value, closer, err := index_s.Get([]byte(tmpR))
			if err != nil {
				// fmt.Println("=====================")
				// log.Printf("Error getting value from index_s: %v", err)
				// fmt.Println("value: ", string(value))
				// fmt.Println("=====================")
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
					value, closer2, err := db_s.Get([]byte(x[:primarySize]))
					// fmt.Println("value: ", string(value))
					if err != nil {
						// log.Printf("Error getting value from db_s: %v", err)
						continue
					}
					dataTime += time.Since(start).Seconds()

					if string(value)[:secondarySize] == tmpR {
						matches++
					}
					closer2.Close()
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
						if err != nil {
							// log.Printf("Error getting value from db_s: %v", err)
							continue
						}
						dataTime += time.Since(start).Seconds()

						if string(value)[:secondarySize] == tmpS {
							matches++
						}
						closer.Close()
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

	// Read the data from the binary files
	dataR, err := readBinaryFile(file_path_r, tuples)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Read %d entries from binary file\n", len(dataR))

	dataS, err := readBinaryFile(file_path_s, tuples)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Read %d entries from binary file\n", len(dataS))

	// Write the data to the Pebble databases
	pk_s, pk_r := generateData(uint64(tuples), uint64(tuples), 0.2, 4, 4, false, 10)
	err = batchWriteDataToPebble(dbR, dataR, pk_r, 100000, "second", 10, 10, totalValueSize)
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
	fmt.Printf("Joining time: %v\n", time.Since(start_time))
	fmt.Printf("!!!!Matches: %d\n", matches)
	dbR.Close()
	dbS.Close()
	
}

func main() {
	NestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 1000000, 50)
	HJ_P("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 1000000, 50)
	// IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", "coveringComp", "Reg", 50)
	// IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", "Comp", "Reg", 50)
	// IndexNestedLoop("/home/weiping/code/lsm_join_data/fb_200M_uint64", "/home/weiping/code/lsm_join_data/fb_200M_uint64", "coveringComp", "Reg")
	// IndexNestedLoop("/home/weiping/code/lsm_join_data/fb_200M_uint64", "/home/weiping/code/lsm_join_data/fb_200M_uint64", "Comp", "Reg")
	// IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 1000000, "Reg", "coveringLazy", 50)
	// IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", "Lazy", "Reg", 50)
	// IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 1000000, "Reg", "Eager", 50)
	// IndexNestedLoop("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 1000000, "Reg", "Lazy", 50)
	// SJ("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 10000000, "Reg", "Eager", 50)
	// SJ("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 1000000, "Reg", "coveringEager", 50)
	// SJ("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id", 1000000, "Reg", "Lazy", 50)

}
