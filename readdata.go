package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"

	// "time"

	"github.com/cockroachdb/pebble"
)

// func main() {
// 	// 打开 Pebble 数据库
// 	dbPath := "demo"

// 	// 如果数据库目录存在，则将其删除
// 	if _, err := os.Stat(dbPath); err == nil {
// 		err = os.RemoveAll(dbPath)
// 		if err != nil {
// 			log.Fatalf("cannot delete existing database: %v", err)
// 		}
// 		fmt.Println("delete existing database")
// 	}
// 	db, err := pebble.Open(dbPath, &pebble.Options{
// 		MemTableSize:                16 << 20, // 256 MB
// 		MemTableStopWritesThreshold: 4,        // 最大并发写缓冲区数量
// 		L0CompactionThreshold:       2,        // 当 Level 0 中有超过 2 个 SST 文件时触发 compaction
// 		L0StopWritesThreshold:       4,
// 		Levels: []pebble.LevelOptions{
// 			{
// 				Compression: pebble.NoCompression, // 禁用 Level 0 的压缩
// 			},
// 			{
// 				Compression: pebble.NoCompression, // 禁用 Level 1 的压缩
// 			},
// 			{
// 				Compression: pebble.NoCompression, // 禁用 Level 2 的压缩
// 			},
// 			{
// 				Compression: pebble.NoCompression, // 禁用 Level 3 的压缩
// 			},
// 			{
// 				Compression: pebble.NoCompression, // 禁用 Level 4 的压缩
// 			},
// 			{
// 				Compression: pebble.NoCompression, // 禁用 Level 5 的压缩
// 			},
// 			{
// 				Compression: pebble.NoCompression, // 禁用 Level 6 的压缩
// 			},
// 			// 根据需要继续添加其他 Levels
// 		},
// 	})
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer db.Close()

// 	// 读取二进制文件
// 	filePath := "/home/weiping/code/lsm_join_data/wiki_ts_200M_uint64"
// 	data, err := readBinaryFile(filePath)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	fmt.Printf("Read %d entries from binary file\n", len(data))

// 	// 将数据批量写入 Pebble 并计时
// 	startTime := time.Now()
// 	err = batchWriteDataToPebble(db, data, 100000, "second", 10, 10, 50) // 批量大小设为 1000
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	duration := time.Since(startTime)

// 	fmt.Printf("Write %d entries to Pebble time: %v\n", len(data), duration)

// 	// 从 Pebble 读取数据
// 	retrievedData, err := readDataFromPebble(db)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	fmt.Printf("Read %d entries from Pebble\n", len(retrievedData))
// 	// fmt.Println(retrievedData[2])
// 	printLevelSizes(db)

// }

func readBinaryFile(filePath string, maxDataCount int) ([]uint64, error) {
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 读取文件内容
	data := make([]uint64, 0)
	buffer := make([]byte, 8)
	for {
		_, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		// 将字节转换为 uint64
		value := binary.LittleEndian.Uint64(buffer)
		data = append(data, value)
		if len(data) >= maxDataCount {
			break
		}
	}

	return data, nil
}

func generateRandomKey(keysize int) int64 {
	min := int64(1)
	for i := 1; i < keysize; i++ {
		min *= 10
	}
	max := min*10 - 1

	return min + rand.Int63n(max-min+1)
}

func batchWriteDataToPebble(db *pebble.DB, data []uint64, pk []uint64, batchSize int, mode string, keysize int, secondsize int, valuesize int) error {
	batch := db.NewBatch()

	for i, value := range data {
		// if i%1000000 == 0 {
		// 	fmt.Printf("writing %d th entry\n", i)
		// }

		var key, valueStr string
		switch mode {
		case "primary":
			// 在 primary 模式下，将 data 作为 key，i 作为 value
			key = fmt.Sprintf("%0*d", keysize, value)    // 使用 value 作为 key，前面补0，长度为 keysize
			valueStr = fmt.Sprintf("%0*d", valuesize, 0) // 使用 value 作为 value，前面补0，长度为 valuesize
		case "second":
			// 在 second 模式下，i 作为 key，data 作为 value 的一部分
			key = fmt.Sprintf("%0*d", keysize, pk[i])
			// fmt.Println("key:", key)
			valueStr = fmt.Sprintf("%0*d", secondsize, value)                   // 使用 value 作为 value 的前半部分，前面补0，长度为 secondsize
			valueStr = fmt.Sprintf("%s%0*d", valueStr, valuesize-secondsize, 0) // value 的后半部分，补0，长度为 valuesize-secondsize
		default:
			return fmt.Errorf("unsupported mode: %s", mode)
		}

		// 将键值对添加到批处理中
		if err := batch.Set([]byte(key), []byte(valueStr), nil); err != nil {
			return err
		}
		// fmt.Println(key,":", valueStr)

		// 当达到批量大小时，提交并重置批处理
		if (i+1)%batchSize == 0 {
			if err := batch.Commit(nil); err != nil {
				return err
			}
			batch.Reset()
		}
	}

	// 提交剩余的数据
	if err := batch.Commit(nil); err != nil {
		return err
	}

	return nil
}

// func writeDataToPebble(db *pebble.DB, data []uint64) error {
// 	// 逐个写入数据
// 	for i, value := range data {
// 		if i%1000000 == 0 {
// 			fmt.Printf("writing %d th entry\n", i)
// 		}
// 		// 创建键（10位字符串）
// 		key := fmt.Sprintf("%010d", i)

// 		// 创建值（20位字符串）
// 		valueStr := fmt.Sprintf("%050d", value)

// 		// 写入键值对
// 		err := db.Set([]byte(key), []byte(valueStr), nil)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

func readDataFromPebble(db *pebble.DB) ([]string, error) {
	// 创建迭代器
	iter, _ := db.NewIter(nil)
	defer iter.Close()

	data := make([]string, 0)

	// 迭代器从第一个元素开始
	for iter.First(); iter.Valid(); iter.Next() {
		// 获取值
		value := iter.Value()

		// 将字符串值转换为uint64并添加到切片中
		numValue := string(value)
		data = append(data, numValue)
	}

	// 检查迭代器是否遇到错误
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return data, nil
}

func printLevelSizes(db *pebble.DB) {
	metrics := db.Metrics()
	fmt.Println("Pebble DB Level Sizes and SST Counts:")

	for i, level := range metrics.Levels {
		fmt.Printf("Level %d: %d bytes, %d SST files\n", i, level.Size, level.NumFiles)
	}
}

func openPebbleDB(dbPath string, memTableSize uint64) (*pebble.DB, error) {
	// 如果数据库目录存在，则将其删除
	if _, err := os.Stat(dbPath); err == nil {
		err = os.RemoveAll(dbPath)
		if err != nil {
			log.Fatalf("cannot delete existing database: %v", err)
		}
		fmt.Println("delete existing database")
	}

	db, err := pebble.Open(dbPath, &pebble.Options{
		MemTableSize:                memTableSize,
		MemTableStopWritesThreshold: 4,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       4,
		Levels: []pebble.LevelOptions{
			{Compression: pebble.NoCompression},
			{Compression: pebble.NoCompression},
			{Compression: pebble.NoCompression},
			{Compression: pebble.NoCompression},
			{Compression: pebble.NoCompression},
			{Compression: pebble.NoCompression},
			{Compression: pebble.NoCompression},
		},
	})
	if err != nil {
		return nil, err
	}

	return db, nil
}
