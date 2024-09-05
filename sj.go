package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

type MinHeapNode struct {
	SecondaryKey string
	PrimaryKey   *string
	I            int
}

// MinHeap is a priority queue implemented as a min-heap
type MinHeap []MinHeapNode

func (h MinHeap) Len() int            { return len(h) }
func (h MinHeap) Less(i, j int) bool  { return h[i].SecondaryKey < h[j].SecondaryKey }
func (h MinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *MinHeap) Push(x interface{}) { *h = append(*h, x.(MinHeapNode)) }
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// ReplaceMin replaces the root of the heap with a new value and heapifies the heap
func (h *MinHeap) ReplaceMin(node MinHeapNode) {
	(*h)[0] = node
	heap.Fix(h, 0)
}

func merge(arr []MinHeapNode, l, m, r int) {
	n1 := m - l + 1
	n2 := r - m

	L := make([]MinHeapNode, n1)
	R := make([]MinHeapNode, n2)

	copy(L, arr[l:m+1])
	copy(R, arr[m+1:r+1])

	i, j, k := 0, 0, l

	for i < n1 && j < n2 {
		if L[i].SecondaryKey <= R[j].SecondaryKey {
			arr[k] = L[i]
			i++
		} else {
			arr[k] = R[j]
			j++
		}
		k++
	}

	for i < n1 {
		arr[k] = L[i]
		i++
		k++
	}

	for j < n2 {
		arr[k] = R[j]
		j++
		k++
	}
}

func mergeSort(arr []MinHeapNode, l, r int) {
	if l < r {
		m := l + (r-l)/2
		mergeSort(arr, l, m)
		mergeSort(arr, m+1, r)
		merge(arr, l, m, r)
	}
}

func mergeFiles(outputFile string, n, k int, prefix string) {
	in := make([]*os.File, k)
	readers := make([]*bufio.Reader, k)

	var err error

	// Open all input files
	for i := 0; i < k; i++ {
		fileName := fmt.Sprintf("%s%d", prefix, i)
		in[i], err = os.Open(fileName)
		if err != nil {
			log.Fatalf("Failed to open file %s: %v", fileName, err)
		}
		readers[i] = bufio.NewReader(in[i])
	}

	out, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer out.Close()

	harr := []MinHeapNode{}
	var line string

	// Initialize the heap with the first line from each file
	for i := 0; i < k; i++ {
		line, err = readLine(readers[i])
		if err == io.EOF {
			continue // Skip empty files
		}
		if err != nil {
			log.Fatalf("Failed to read line from file %s: %v", in[i].Name(), err)
		}
		fields := strings.Split(line, ",")
		fmt.Println("i", i, "fields: ", fields, len(fields))
		if len(fields) == 2 {
			harr = append(harr, MinHeapNode{SecondaryKey: fields[0], PrimaryKey: &fields[1], I: i})
			fmt.Println("i", i, "SecondaryKey: ", harr[i].SecondaryKey, "PrimaryKey: ", *harr[i].PrimaryKey, len(harr))
		}
	}
	fmt.Println(len(harr))

	if len(harr) == 0 {
		log.Println("No valid input files with data")
		return
	}

	h := MinHeap(harr)
	heap.Init(&h)

	// Process the heap and merge the files
	for len(h) > 0 {
		root := heap.Pop(&h).(MinHeapNode)
		out.WriteString(fmt.Sprintf("%s,%s\n", root.SecondaryKey, *root.PrimaryKey))

		line, err = readLine(readers[root.I])
		if err != nil {
			if err == io.EOF {
				continue // Skip files that have reached EOF
			}
			log.Fatalf("Failed to read line from file %s: %v", in[root.I].Name(), err)
		}

		fields := strings.Split(line, ",")
		if len(fields) == 2 {
			root.SecondaryKey = fields[0]
			root.PrimaryKey = &fields[1]
			heap.Push(&h, root)
		}
	}

	// Close all input files
	for _, f := range in {
		f.Close()
	}
}

func SortMergeForEagerLazy(dbR, dbS *pebble.DB, itR, itS *pebble.Iterator, RIndex string, SIndex string, primarySize int, secondarySize int, totalValueSize int) int {
	matches := 0
	count1, count2 := 0, 0
	dataTime, indexTime, postTime := 0.0, 0.0, 0.0

	for itR.Valid() && itS.Valid() {
		tempRKey := string(itR.Key())
		tempSKey := string(itS.Key())

		if tempRKey == tempSKey {
			tempRValue := string(itR.Value())
			tempSValue := string(itS.Value())

			timer := time.Now()
			postTime += time.Since(timer).Seconds()

			if strings.Contains(RIndex, "covering") {
				valueSplitR := SplitStringEveryNChars(tempRValue, totalValueSize)
				// fmt.Println("valueSplitR: ", valueSplitR)
				count1 += len(valueSplitR)
			} else {
				timer = time.Now()
				valueSplitR := SplitStringEveryNChars(tempRValue, primarySize)
				uniqueValues := make(map[string]struct{})
				for chunk := range valueSplitR {
					uniqueValues[chunk] = struct{}{}
				}
				postTime += time.Since(timer).Seconds()

				for x := range uniqueValues {
					timer = time.Now()
					valueR, closer, err := dbR.Get([]byte(x[:primarySize]))
					if err == nil {
						closer.Close()
					}
					dataTime += time.Since(timer).Seconds()

					isMatch := err == nil && string(valueR)[:secondarySize] == tempRKey
					if isMatch {
						count1++
					}
				}
			}

			timer = time.Now()
			postTime += time.Since(timer).Seconds()

			if strings.Contains(SIndex, "covering") {
				valueSplitS := SplitStringEveryNChars(tempSValue, totalValueSize)

				count2 += len(valueSplitS)
			} else {
				timer = time.Now()
				valueSplitS := SplitStringEveryNChars(tempSValue, primarySize)

				uniqueValues := make(map[string]struct{})
				for chunk := range valueSplitS {
					uniqueValues[chunk] = struct{}{}
				}
				postTime += time.Since(timer).Seconds()

				for x := range uniqueValues {
					timer = time.Now()
					valueS, closer, err := dbS.Get([]byte(x[:primarySize]))
					if err == nil && len(valueS) > 0 {
						defer closer.Close()
					}
					dataTime += time.Since(timer).Seconds()

					isMatch := err == nil && string(valueS)[:secondarySize] == tempSKey
					if isMatch {
						count2++
					}
				}
			}

			matches += count1 * count2
			count1 = 0
			count2 = 0
			indexTime += time.Since(timer).Seconds()
			itR.Next()
			itS.Next()
		} else if tempRKey < tempSKey {
			itR.Next()
		} else {
			itS.Next()
		}
	}

	return matches
}

func SortMergeForComp(dbR, dbS *pebble.DB, itR, itS *pebble.Iterator, RIndex string, SIndex string, primarySize int, secondarySize int) int {
	matches := 0
	count1, count2 := 0, 0
	dataTime, indexTime := 0.0, 0.0

	for itR.Valid() && itS.Valid() {
		rKeyStr := string(itR.Key())
		sKeyStr := string(itS.Key())

		tempRKey := rKeyStr[:secondarySize]
		tempSKey := sKeyStr[:secondarySize]

		if tempRKey == tempSKey {
			tempRValue := rKeyStr[secondarySize : secondarySize+primarySize]
			tempSValue := sKeyStr[secondarySize : secondarySize+primarySize]

			if strings.Contains(RIndex, "covering") || strings.Contains(RIndex, "Reg") {
				count1++
			} else {
				timer := time.Now()
				valueR, closer, err := dbR.Get([]byte(tempRValue))
				if err == nil && len(valueR) > 0 {
					defer closer.Close()
				}
				dataTime += time.Since(timer).Seconds()

				isMatch := err == nil && string(valueR)[:secondarySize] == tempRKey
				if isMatch {
					count1++
				}
			}

			if strings.Contains(SIndex, "covering") || strings.Contains(SIndex, "Reg") {
				count2++
			} else {
				timer := time.Now()
				valueS, closer, err := dbS.Get([]byte(tempSValue))
				if err == nil && len(valueS) > 0 {
					defer closer.Close()
				}
				dataTime += time.Since(timer).Seconds()

				isMatch := err == nil && string(valueS)[:secondarySize] == tempSKey
				if isMatch {
					count2++
				}
			}

			if !strings.Contains(RIndex, "Reg") {
				tmp := tempRKey
				for itR.Valid() {
					timer := time.Now()
					itR.Next()
					indexTime += time.Since(timer).Seconds()
					if !itR.Valid() {
						break
					}

					tempRKey = string(itR.Key())[:secondarySize]
					if tempRKey != tmp {
						break
					}

					tempRValue = string(itR.Key())[secondarySize : secondarySize+primarySize]
					if strings.Contains(SIndex, "covering") {
						count1++
					} else {
						timer = time.Now()
						valueR, closer, err := dbR.Get([]byte(tempRValue))
						if err == nil && len(valueR) > 0 {
							defer closer.Close()
						}
						dataTime += time.Since(timer).Seconds()

						isMatch := err == nil && string(valueR)[:secondarySize] == tempRKey
						if isMatch {
							count1++
						}
					}
				}
			}

			if !strings.Contains(SIndex, "Reg") {
				tmp := tempSKey
				for itS.Valid() {
					timer := time.Now()
					itS.Next()
					indexTime += time.Since(timer).Seconds()
					if !itS.Valid() {
						break
					}

					tempSKey = string(itS.Key())[:secondarySize]
					if tempSKey != tmp {
						break
					}

					tempSValue = string(itS.Key())[secondarySize : secondarySize+primarySize]
					if strings.Contains(SIndex, "covering") {
						count2++
					} else {
						timer = time.Now()
						valueS, closer, err := dbS.Get([]byte(tempSValue))
						if err == nil && len(valueS) > 0 {
							defer closer.Close()
						}
						dataTime += time.Since(timer).Seconds()

						isMatch := err == nil && string(valueS)[:secondarySize] == tempSKey
						if isMatch {
							count2++
						}
					}
				}
			}

			matches += count1 * count2
			count1 = 0
			count2 = 0
		} else if tempRKey < tempSKey {
			timer := time.Now()
			itR.Next()
			indexTime += time.Since(timer).Seconds()
		} else {
			timer := time.Now()
			itS.Next()
			indexTime += time.Since(timer).Seconds()
		}
	}

	return matches
}

func SortMerge(dbR, dbS *pebble.DB, tuples int, itR, itS *pebble.Iterator, RIndex string, SIndex string, primarySize int, secondarySize int, totalValueSize int) {

	// Seek both iterators to the first entry
	itR.First()
	itS.First()
	matches := 0
	fmt.Println("RIndex: ", RIndex, "SIndex: ", SIndex)
	// Determine which specialized sort merge join to use
	start_time := time.Now()
	if strings.Contains(RIndex, "Reg") {
		fmt.Println("Sort merge with Reg")
		matches = SingleIndexExternalSortMerge(dbR, dbS, tuples, itS, SIndex, primarySize, secondarySize, totalValueSize)
	} else if (strings.Contains(RIndex, "Lazy") && strings.Contains(SIndex, "Lazy")) || (strings.Contains(SIndex, "Eager") && strings.Contains(RIndex, "Eager")) {
		fmt.Println("Sort merge with Eager/Lazy")
		matches = SortMergeForEagerLazy(dbR, dbS, itR, itS, RIndex, SIndex, primarySize, secondarySize, totalValueSize)
	} else if strings.Contains(RIndex, "Comp") && strings.Contains(SIndex, "Comp") {
		fmt.Println("Sort merge with Comp")
		matches = SortMergeForComp(dbR, dbS, itR, itS, RIndex, SIndex, primarySize, secondarySize)
	} else {
		fmt.Println("Not supported")
	}

	fmt.Println("!!!!Joining Time: ", time.Since(start_time))
	fmt.Println("!!!!Matches: ", matches)
	// Close iterators
	itR.Close()
	itS.Close()
}

func NonIndexExternalSortMerge(dbR, dbS *pebble.DB, tuples int, RIndex string, SIndex string, primarySize int, secondarySize int, totalValueSize int) {
	fmt.Println("Performing external sort merge for non-indexed data...")

	valueSize := totalValueSize

	// Serialize data
	runSize := int((16<<20-3*4096)/(primarySize+valueSize)/2) - 1
	fmt.Println("Run size: ", runSize)
	// Sort R
	fmt.Println("Sorting R...")
	prefixR := "R/_sj_output"
	outputFileR := prefixR + ".txt"
	numWaysR := tuples/runSize + 1
	fmt.Println("Num ways R: ", numWaysR)
	externalSort(dbR, outputFileR, numWaysR, runSize, valueSize, secondarySize, prefixR)

	// Sort S
	fmt.Println("Sorting S...")
	prefixS := "S/_sj_output"
	outputFileS := prefixS + ".txt"
	numWaysS := tuples/runSize + 1
	externalSort(dbS, outputFileS, numWaysS, runSize, valueSize, secondarySize, prefixS)

	matches := 0
	inR, err := os.Open(outputFileR)
	if err != nil {
		log.Fatalf("Unable to open file R: %v", err)
	}
	defer inR.Close()
	fmt.Println("Opened file R")

	inS, err := os.Open(outputFileS)
	if err != nil {
		log.Fatalf("Unable to open file S: %v", err)
	}
	defer inS.Close()
	fmt.Println("Opened file S")

	readerR := bufio.NewReader(inR)
	readerS := bufio.NewReader(inS)

	lineR, _ := readerR.ReadString('\n')
	lineS, _ := readerS.ReadString('\n')

	count1, count2 := 1, 1
	countS, countR := 0, 0
	// timer := time.Now()
	termialR, termialS := false, false
	for len(lineR) > 0 && len(lineS) > 0 {
		fieldsR := strings.Split(lineR, ",")
		fieldsS := strings.Split(lineS, ",")
		if len(fieldsR) < 2 || len(fieldsS) < 2 {
			break
		}
		if termialR && termialS {
			break
		}

		tempRKey := fieldsR[0]
		// tempRValue := fieldsR[1]
		tempSKey := fieldsS[0]
		// tempSValue := fieldsS[1]

		if tempRKey == tempSKey {
			for {
				nextLineR, err := readerR.ReadString('\n')
				countR++
				// fmt.Println("nextLineR: ", nextLineR)
				if err != nil {
					termialR = true
					break
				}
				nextFieldsR := strings.Split(nextLineR, ",")
				// fmt.Println("nextFieldsR: ", len(nextFieldsR))
				if len(nextFieldsR) < 2 || nextFieldsR[0] != tempRKey {
					lineR = nextLineR
					break
				}
				count1++
			}

			for {
				nextLineS, err := readerS.ReadString('\n')
				countS++
				// fmt.Println("nextLineS: ", nextLineS)

				if err != nil {
					termialS = true
					break
				}
				nextFieldsS := strings.Split(nextLineS, ",")
				// fmt.Println("nextFieldsR: ", len(nextFieldsS))

				if len(nextFieldsS) < 2 || nextFieldsS[0] != tempSKey {
					lineS = nextLineS
					break
				}
				count2++
			}
			matches += count1 * count2
			count1 = 1
			count2 = 1
		} else if tempRKey < tempSKey {
			lineR, _ = readerR.ReadString('\n')
		} else {
			lineS, _ = readerS.ReadString('\n')
		}
	}
	fmt.Println("!!!!Matches: ", matches)
}

func SingleIndexExternalSortMerge(dbR, dbS *pebble.DB, tuples int, itS *pebble.Iterator, SIndex string, primarySize int, secondarySize int, totalValueSize int) int {
	fmt.Println("Performing external sort merge with single index...")

	// Serialize data
	runSize := int((16<<20-3*4096)/(primarySize+totalValueSize)/2) - 1

	prefixR := "R/_sj_output"
	outputFileR := prefixR + ".txt"
	numWaysR := tuples/runSize + 1
	fmt.Println("Num ways R: ", numWaysR)
	externalSort(dbR, outputFileR, numWaysR, runSize, totalValueSize, secondarySize, prefixR)
	// Perform external sort on R
	matches := 0
	inR, err := os.Open(outputFileR)
	if err != nil {
		log.Fatalf("Unable to open file R: %v", err)
	}
	defer inR.Close()

	readerR := bufio.NewReader(inR)
	lineR, err := readerR.ReadString('\n')
	if err != nil {
		log.Fatalf("Failed to read line from file R: %v", err)
	}

	// timer := time.Now()
	count1, count2 := 1, 0
	// count :=0

	for itS.Valid() && len(lineR) > 0 {
		fieldsR := strings.Split(lineR, ",")
		if len(fieldsR) < 2 {
			break
		}
		tempRKey := fieldsR[0]
		// tempRValue := fieldsR[1]

		tempSKey := string(itS.Key()[:secondarySize])
		tempPKey := string(itS.Key()[secondarySize:])
		tempSValue := string(itS.Value())
		// fmt.Println(count, "RKey:", tempRKey, "SKey: ", tempSKey, tempRKey == tempSKey)
		if tempRKey == tempSKey {
			for {
				nextLineR, err := readerR.ReadString('\n')
				if err != nil {
					break
				}
				nextFieldsR := strings.Split(nextLineR, ",")
				if len(nextFieldsR) < 2 || nextFieldsR[0] != tempRKey {
					lineR = nextLineR
					break
				}
				count1++
			}

			if strings.Contains(SIndex, "Comp") {
				if strings.Contains(SIndex, "covering") {
					count2++
				} else {
					valueS, closer, _ := dbS.Get([]byte(tempPKey))
					if string(valueS)[:secondarySize] == tempSKey {
						count2++
						defer closer.Close()
					}
				}
				for itS.Next(); itS.Valid(); itS.Next() {
					if tempSKey != string(itS.Key()[:secondarySize]) {
						break
					}
					if strings.Contains(SIndex, "covering") {
						count2++
					} else {
						tempPKey := string(itS.Key()[secondarySize:])
						valueS, closer, _ := dbS.Get([]byte(tempPKey))
						if string(valueS)[:secondarySize] == tempSKey {
							count2++
							defer closer.Close()
						}
					}
				}
			} else {
				if strings.Contains(SIndex, "covering") {
					valueSplit := SplitStringEveryNChars(tempSValue, totalValueSize)
					count2 += len(valueSplit)
				} else {
					valueSplit := SplitStringEveryNChars(tempSValue, primarySize)
					for x := range valueSplit {
						valueS, closer, err1 := dbS.Get([]byte(x[:primarySize]))
						if err1 == nil {
							closer.Close()
						}
						if len(valueS) > 0 && string(valueS)[:secondarySize] == tempSKey {
							count2++
						}
					}
				}
			}
			// fmt.Println("count1: ", count1, "count2: ", count2)
			matches += count1 * count2
			count1 = 1
			count2 = 0
		} else if tempRKey < tempSKey {
			lineR, err = readerR.ReadString('\n')
			if err != nil {
				break
			}
		} else {
			itS.Next()
		}
	}
	return matches
}

func SJ(file_path_r string, file_path_s string, tuples int, rIndex string, sIndex string, totalValueSize int) {
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

	// batchWriteDataToPebble(db_r, data_r, pk_r, 100000, "second", 10, 10, 50)
	//
	// fmt.Printf("Write %d entries to Pebble R time: %v\n", len(data_r), duration)

	// data_s, err := readBinaryFile(file_path_s, tuples)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	start_time := time.Now()
	index_r, _ := openPebbleDB("R_Index", 16<<20)
	defer index_r.Close()
	if strings.Contains(rIndex, "Comp") {
		if strings.Contains(rIndex, "covering") {
			buildCoveringCompositeIndex(db_r, index_r, data_r, pk_r, totalValueSize, 10, 10, false, 1000)
		} else {
			_ = batchWriteDataToPebble(db_r, data_r, pk_r, 100000, "second", 10, 10, totalValueSize)
			buildCompositeIndex(index_r, data_r, pk_r, 10, 10, 1000)
		}
	}

	if strings.Contains(rIndex, "Lazy") || strings.Contains(rIndex, "Eager") {
		if strings.Contains(rIndex, "covering") {
			if strings.Contains(rIndex, "Lazy") {
				buildCoveringLazyIndex(db_r, index_r, data_r, pk_r, totalValueSize, 10, 10, false, 1000)
			} else {
				buildCoveringEagerIndex(db_r, index_r, data_r, pk_r, totalValueSize, 10, 10, false, 1000)
			}
		} else {
			_ = batchWriteDataToPebble(db_r, data_r, pk_r, 100000, "second", 10, 10, totalValueSize)
			if strings.Contains(sIndex, "Lazy") {
				buildLazyIndex(index_r, data_r, pk_r, totalValueSize, 10, 10, 1000)
			} else {
				buildEagerIndex(index_r, data_r, pk_r, totalValueSize, 10, 10, 1000)
			}
		}
	}
	index_time := time.Since(start_time)

	if strings.Contains(rIndex, "Reg") {
		_ = batchWriteDataToPebble(db_r, data_r, pk_r, 100000, "second", 10, 10, totalValueSize)
	}

	start_time = time.Now()
	index_s, _ := openPebbleDB("S_Index", 16<<20)
	defer index_s.Close()
	if strings.Contains(sIndex, "Comp") {
		if strings.Contains(sIndex, "covering") {
			buildCoveringCompositeIndex(db_s, index_s, data_s, pk_s, totalValueSize, 10, 10, false, 1000)
		} else {
			_ = batchWriteDataToPebble(db_s, data_s, pk_s, 100000, "second", 10, 10, totalValueSize)
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
			if strings.Contains(sIndex, "Lazy") {
				buildLazyIndex(index_s, data_s, pk_s, totalValueSize, 10, 10, 1000)
			} else {
				buildEagerIndex(index_s, data_s, pk_s, totalValueSize, 10, 10, 1000)
			}
		}
	}
	index_time += time.Since(start_time)
	fmt.Printf("!!!!Index time: %v\n", index_time)

	if strings.Contains(sIndex, "Reg") {
		_ = batchWriteDataToPebble(db_s, data_s, pk_s, 100000, "second", 10, 10, totalValueSize)
	}

	fmt.Println("Sort merge joining...")

	readOptions := &pebble.IterOptions{}
	itR, _ := index_r.NewIter(readOptions)
	if strings.Contains(rIndex, "Reg") {
		itR, _ = db_r.NewIter(readOptions)
	}
	defer itR.Close()

	itS, _ := index_s.NewIter(readOptions)
	defer itS.Close()

	if !strings.Contains(rIndex, "Reg") && !strings.Contains(sIndex, "Reg") {
		SortMerge(db_r, db_s, tuples, itR, itS, rIndex, sIndex, 10, 10, totalValueSize)
	} else {
		NonIndexExternalSortMerge(db_r, db_s, tuples, rIndex, sIndex, 10, 10, totalValueSize)
	}

}

func SJ_P(file_path_r, file_path_s string, tuples int, totalValueSize int) {
	fmt.Println("Performing external sort merge with primary index...")
	db_r, _ := openPebbleDB("R", 16<<20)
	defer db_r.Close()
	db_s, _ := openPebbleDB("S", 16<<20)
	defer db_s.Close()

	pk_s, pk_r := generateData(uint64(tuples), uint64(tuples), 0.2, 1, 1, false, 10)
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

	start_time := time.Now()

	readOptions := &pebble.IterOptions{}
	it_s, _ := db_s.NewIter(readOptions)
	defer it_s.Close()
	it_s.First()
	// Serialize data
	runSize := int((16<<20-3*4096)/(10+totalValueSize)/2) - 1

	prefixR := "R/_sj_output"
	outputFileR := prefixR + ".txt"
	numWaysR := tuples/runSize + 1
	fmt.Println("Num ways R: ", numWaysR)
	externalSort(db_r, outputFileR, numWaysR, runSize, 10, 10, prefixR)
	// Perform external sort on R
	matches := 0
	inR, err := os.Open(outputFileR)
	if err != nil {
		log.Fatalf("Unable to open file R: %v", err)
	}
	defer inR.Close()

	readerR := bufio.NewReader(inR)
	lineR, err := readerR.ReadString('\n')
	if err != nil {
		log.Fatalf("Failed to read line from file R: %v", err)
	}

	count1, count2 := 1, 0
	// count :=0

	for it_s.Valid() && len(lineR) > 0 {
		fieldsR := strings.Split(lineR, ",")
		// fmt.Println("fieldsR: ", fieldsR)
		if len(fieldsR) < 2 {
			break
		}
		tempRKey := fieldsR[0]
		// tempRValue := fieldsR[1]

		tempSKey := string(it_s.Key()[:10])
		// fmt.Println("RKey:", tempRKey, "SKey: ", tempSKey, tempRKey == tempSKey)
		if tempRKey == tempSKey {
			for {
				nextLineR, err := readerR.ReadString('\n')
				if err != nil {
					break
				}
				nextFieldsR := strings.Split(nextLineR, ",")
				if len(nextFieldsR) < 2 || nextFieldsR[0] != tempRKey {
					lineR = nextLineR
					break
				}
				count1++
			}

			count2++

			for it_s.Next(); it_s.Valid(); it_s.Next() {
				if tempSKey != string(it_s.Key()[:10]) {
					break
				}
				count2++

			}

			// fmt.Println("count1: ", count1, "count2: ", count2)
			matches += count1 * count2
			count1 = 1
			count2 = 0
		} else if tempRKey < tempSKey {
			lineR, err = readerR.ReadString('\n')
			if err != nil {
				break
			}
		} else {
			it_s.Next()
		}
	}
	duration := time.Since(start_time)
	fmt.Printf("!!!!Joining time: %v\n", duration)
	fmt.Println("!!!!Matches: ", matches)
}

func SJ_NP(file_path_r, file_path_s string, tuples int, rIndex string, totalValueSize int) {
	fmt.Println("Performing external sort merge with single index...")
	primarySize := 10
	secondarySize := 10
	db_r, _ := openPebbleDB("R", 16<<20)
	defer db_r.Close()
	db_s, _ := openPebbleDB("S", 16<<20)
	defer db_s.Close()

	pk_s, pk_r := generateData(uint64(tuples), uint64(tuples), 0.2, 1, 1, false, 10)
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
	// batchWriteDataToPebble(db_r, dataR, pk_r, 100000, "second", 10, 10, totalValueSize)
	batchWriteDataToPebble(db_s, dataS, pk_s, 100000, "primary", 10, 10, totalValueSize)

	// Serialize data
	startTime := time.Now()
	index_r, _ := openPebbleDB("R_Index", 16<<20)
	defer index_r.Close()
	if strings.Contains(rIndex, "Comp") {
		if strings.Contains(rIndex, "covering") {
			buildCoveringCompositeIndex(db_r, index_r, dataR, pk_r, totalValueSize, 10, 10, false, 1000)
		} else {
			_ = batchWriteDataToPebble(db_r, dataR, pk_r, 100000, "second", 10, 10, totalValueSize)
			buildCompositeIndex(index_r, dataR, pk_r, 10, 10, 1000)
		}
	}

	if strings.Contains(rIndex, "Lazy") || strings.Contains(rIndex, "Eager") {
		if strings.Contains(rIndex, "covering") {
			if strings.Contains(rIndex, "Lazy") {
				buildCoveringLazyIndex(db_r, index_r, dataR, pk_s, totalValueSize, 10, 10, false, 1000)
			} else {
				buildCoveringEagerIndex(db_r, index_r, dataR, pk_s, totalValueSize, 10, 10, false, 1000)
			}
		} else {
			_ = batchWriteDataToPebble(db_r, dataR, pk_r, 100000, "second", 10, 10, totalValueSize)
			// fmt.Printf("Write %d entries to Pebble S time: %v\n", len(dataR), duration)
			if strings.Contains(rIndex, "Lazy") {
				buildLazyIndex(index_r, dataR, pk_r, totalValueSize, 10, 10, 1000)
			} else {
				buildEagerIndex(index_r, dataR, pk_r, totalValueSize, 10, 10, 1000)
			}
		}
	}
	index_duration := time.Since(startTime)
	// Perform external sort on R
	matches := 0

	// timer := time.Now()
	count1, count2 := 1, 0
	// count :=0
	itS, _ := db_s.NewIter(nil)
	itS.First()
	itR, _ := index_r.NewIter(nil)
	itR.First()
	defer itR.Close()
	defer itS.Close()

	fmt.Println("Sort merge joining...")
	startTime = time.Now()
	for itS.Valid() && itR.Valid() {
		tempSKey := string(itS.Key()[:secondarySize])
		// tempRValue := fieldsR[1]

		tempRKey := string(itR.Key()[:secondarySize])
		tempPKey := string(itR.Key()[secondarySize:])
		tempRValue := string(itR.Value())
		if tempRKey == tempSKey {
			for {
				itS.Next()
				if string(itS.Key()[:secondarySize]) != tempRKey || !itS.Valid() {
					break
				}
				count1++
			}
			if strings.Contains(rIndex, "Comp") {
				if strings.Contains(rIndex, "covering") {
					count2++
				} else {
					valueS, closer, err := db_r.Get([]byte(tempPKey))
					if err == nil {
						closer.Close()
					}
					if string(valueS)[:secondarySize] == tempSKey {
						count2++
					}
				}
				for itR.Next(); itR.Valid(); itR.Next() {
					if tempSKey != string(itR.Key()[:secondarySize]) {
						break
					}
					if strings.Contains(rIndex, "covering") {
						count2++
					} else {
						tempPKey := string(itR.Key()[secondarySize:])
						valueS, closer, err := db_r.Get([]byte(tempPKey))
						if err == nil {
							closer.Close()
						}
						if string(valueS)[:secondarySize] == tempRKey {
							count2++
						}
					}
				}
			} else {
				if strings.Contains(rIndex, "covering") {
					valueSplit := SplitStringEveryNChars(tempRValue, totalValueSize)
					count2 += len(valueSplit)
				} else {
					valueSplit := SplitStringEveryNChars(tempRValue, primarySize)
					for x := range valueSplit {
						valueS, closer, err := db_r.Get([]byte(x[:primarySize]))
						if err == nil {
							closer.Close()
						}
						if len(valueS) > 0 && string(valueS)[:secondarySize] == tempSKey {
							count2++
						}
					}
				}
			}
			// fmt.Println("count1: ", count1, "count2: ", count2)
			matches += count1 * count2
			count1 = 1
			count2 = 0
		} else if tempRKey < tempSKey {
			itR.Next()
		} else {
			itS.Next()
		}
	}
	fmt.Println("!!!!Indexing time: ", index_duration)
	fmt.Println("!!!!Joining time: ", time.Since(startTime))
	fmt.Println("!!!!Matches: ", matches)
}
