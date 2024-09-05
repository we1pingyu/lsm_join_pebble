package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

func HJ(file_path_r string, file_path_s string, tuples int, totalValueSize int) {
	db_r, _ := openPebbleDB("R", 16<<20)
	db_s, _ := openPebbleDB("S", 16<<20)

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
	fmt.Printf("Read %d entries from binary file\n", len(data_r))
	startTime := time.Now()
	batchWriteDataToPebble(db_r, data_r, pk_r, 100000, "second", 10, 10, totalValueSize)

	duration := time.Since(startTime)
	fmt.Printf("Write %d entries to Pebble R time: %v\n", len(data_r), duration)

	startTime = time.Now()
	batchWriteDataToPebble(db_s, data_s, pk_s, 100000, "second", 10, 10, totalValueSize)

	duration = time.Since(startTime)
	fmt.Printf("Write %d entries to Pebble S time: %v\n", len(data_s), duration)

	// 分区和探测
	numBuckets := 500
	prefixR := "tmp/R_hj"
	prefixS := "tmp/S_hj"

	startTime = time.Now()
	partitioning(db_r, prefixR, numBuckets, totalValueSize, 10, "second")
	partitioning(db_s, prefixS, numBuckets, totalValueSize, 10, "second")
	matches := probing(numBuckets, prefixR, prefixS)
	duration = time.Since(startTime)
	fmt.Printf("Hash join time: %v\n", duration)
	fmt.Printf("Total matches found: %d\n", matches)

	// 打印数据库状态
	// printLevelSizes(db_r)
	// printLevelSizes(db_s)
	db_r.Close()
	db_s.Close()
}

func partitioning(db *pebble.DB, prefix string, numBuckets int, valueSize int, secondarySize int, indexType string) {
	outFiles := make([]*os.File, numBuckets)
	for i := 0; i < numBuckets; i++ {
		fileName := fmt.Sprintf("%s_%d", prefix, i)
		file, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("Failed to create file: %v", err)
		}
		outFiles[i] = file
		defer file.Close()
	}

	iter, _ := db.NewIter(nil)
	defer iter.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		var secondaryKey, primaryKey string
		if indexType == "primary" {
			secondaryKey = string(key)
			primaryKey = string(value)
		} else {
			secondaryKey = string(value[:secondarySize])
			primaryKey = string(key) + string(value[secondarySize:])
		}

		hash := BKDRhash2(secondaryKey, numBuckets)
		outFiles[hash].WriteString(fmt.Sprintf("%s,%s\n", secondaryKey, primaryKey))
		count++
		// fmt.Printf("Secondary key: %s, Primary key: %s\n", secondaryKey, primaryKey)
	}
	fmt.Printf("Total entries: %d\n", count)
}

func probing(numBuckets int, prefixR string, prefixS string) uint64 {
	var matches uint64 = 0

	for i := 0; i < numBuckets; i++ {
		rFile, err := os.Open(fmt.Sprintf("%s_%d", prefixR, i))
		if err != nil {
			log.Fatalf("Failed to open R file: %v", err)
		}
		defer rFile.Close()

		sFile, err := os.Open(fmt.Sprintf("%s_%d", prefixS, i))
		if err != nil {
			log.Fatalf("Failed to open S file: %v", err)
		}
		defer sFile.Close()

		rMap := make(map[string]int)
		scanner := bufio.NewScanner(rFile)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, ",")
			key := parts[0]

			// 增加该键的计数
			rMap[key]++
		}

		// 在 S 文件中进行探测并计算匹配次数
		scanner = bufio.NewScanner(sFile)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, ",")
			key := parts[0]

			if count, exists := rMap[key]; exists {
				// 增加 matches，根据 rMap 中存储的次数来累加
				matches += uint64(count)
			}
		}
	}

	return matches
}

func BKDRhash2(str string, buckets int) int {
	var hash uint32
	seed := uint32(131)
	for i := 0; i < len(str); i++ {
		hash = hash*seed + uint32(str[i])
	}
	return int(hash) % buckets
}

func HJ_P(file_path_r string, file_path_s string, tuples int, totalValueSize int) {
	db_r, _ := openPebbleDB("R", 16<<20)
	db_s, _ := openPebbleDB("S", 16<<20)
	pk_s, pk_r := generateData(uint64(tuples), uint64(tuples), 0.2, 1, 1, false, totalValueSize)

	// data_r, err := readBinaryFile(file_path_r, tuples)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Printf("Read %d entries from binary file\n", len(data_r))
	// startTime := time.Now()
	// err = batchWriteDataToPebble(db_r, data_r, pk_r, 100000, "second", 10, 10, totalValueSize)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// duration := time.Since(startTime)
	// fmt.Printf("Write %d entries to Pebble R time: %v\n", len(data_r), duration)

	// data_s, err := readBinaryFile(file_path_s, tuples)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// startTime = time.Now()
	// err = batchWriteDataToPebble(db_s, data_s, pk_s, 100000, "primary", 10, 10, totalValueSize)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// duration = time.Since(startTime)
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
	batchWriteDataToPebble(db_r, dataR, pk_r, 100000, "second", 10, 10, totalValueSize)
	batchWriteDataToPebble(db_s, dataS, pk_s, 100000, "primary", 10, 10, totalValueSize)

	// count := 0
	// itS, _ := db_s.NewIter(nil)
	// for itS.First(); itS.Valid(); itS.Next() {
	// 	count++
	// }
	// fmt.Printf("Total entries in S: %d\n", count)

	// 分区和探测
	numBuckets := 500
	prefixR := "tmp/R_hj"
	prefixS := "tmp/S_hj"

	startTime := time.Now()
	partitioning(db_r, prefixR, numBuckets, totalValueSize, 10, "second")
	partitioning(db_s, prefixS, numBuckets, totalValueSize, 10, "primary")
	matches := probing(numBuckets, prefixR, prefixS)
	duration := time.Since(startTime)
	fmt.Printf("!!!!Joining time: %v\n", duration)
	fmt.Printf("!!!!Matches: %d\n", matches)

	// 打印数据库状态
	// printLevelSizes(db_r)
	// printLevelSizes(db_s)
	db_r.Close()
	db_s.Close()
}

// func main() {
// 	HJ_P("/home/weiping/code/lsm_join_data/movie_info_movie_id", "/home/weiping/code/lsm_join_data/cast_info_movie_id")
// 	HJ_P("/home/weiping/code/lsm_join_data/question_user_id", "/home/weiping/code/lsm_join_data/so_user_user_id")
// 	HJ_P("/home/weiping/code/lsm_join_data/wiki_ts_200M_uint64", "/home/weiping/code/lsm_join_data/wiki_ts_200M_uint64")
// 	HJ_P("/home/weiping/code/lsm_join_data/fb_200M_uint64", "/home/weiping/code/lsm_join_data/fb_200M_uint64")
// }
