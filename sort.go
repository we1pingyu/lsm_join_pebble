package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
)

func externalSort(db *pebble.DB, outputFile string, numWays, runSize, valueSize, secondarySize int, prefix string) {
	// Step 1: Create initial sorted runs
	createInitialRuns(db, runSize, numWays, prefix)

	// Step 2: Merge the sorted runs into the final output file
	mergeSortedRuns(outputFile, numWays, prefix)
}

// createInitialRuns generates sorted runs from the database and writes them to temporary files
func createInitialRuns(db *pebble.DB, runSize, numWays int, prefix string) {
	it, _ := db.NewIter(nil)
	defer it.Close()

	// count := 0
	run := 0
	var batch []string

	for it.First(); it.Valid(); it.Next() {
		key := string(it.Value()[:10])
		value := string(it.Key()) + string(it.Value()[10:])
		batch = append(batch, fmt.Sprintf("%s,%s", key, value))

		if len(batch) == runSize {
			sort.Strings(batch)
			writeRunToFile(batch, run, prefix)
			run++
			batch = nil // Reset the batch
		}
	}

	// Write remaining batch
	if len(batch) > 0 {
		sort.Strings(batch)
		writeRunToFile(batch, run, prefix)
		run++
	}

	// Log the number of runs created
	log.Printf("Created %d initial sorted runs\n", run)
}

// writeRunToFile writes a sorted batch to a file
func writeRunToFile(batch []string, run int, prefix string) {
	fileName := fmt.Sprintf("%s%d", prefix, run)
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Failed to create run file %s: %v", fileName, err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, line := range batch {
		writer.WriteString(line + "\n")
	}
	writer.Flush()
}

// mergeSortedRuns merges sorted runs into a single sorted output file
func mergeSortedRuns(outputFile string, numWays int, prefix string) {
	files := make([]*os.File, 0, numWays)
	readers := make([]*bufio.Reader, 0, numWays)
	lines := make([]string, 0, numWays)
	errs := make([]error, 0, numWays)

	// Open all run files that exist and initialize readers
	for i := 0; i < numWays; i++ {
		fileName := fmt.Sprintf("%s%d", prefix, i)
		file, err := os.Open(fileName)
		if os.IsNotExist(err) {
			// Skip this file as it doesn't exist
			continue
		} else if err != nil {
			log.Fatalf("Failed to open run file %s: %v", fileName, err)
		}
		files = append(files, file)
		readers = append(readers, bufio.NewReader(file))

		line, err := readLine(readers[len(readers)-1])
		if err != nil && err != io.EOF {
			log.Fatalf("Failed to read line from run file %s: %v", fileName, err)
		}
		lines = append(lines, line)
		errs = append(errs, err)
	}

	if len(files) == 0 {
		log.Println("No valid input files with data to merge")
		return
	}

	output, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file %s: %v", outputFile, err)
	}
	defer output.Close()

	writer := bufio.NewWriter(output)

	for {
		// Find the smallest line among the readers
		minIndex := -1
		for i := 0; i < len(readers); i++ {
			if errs[i] == io.EOF {
				continue
			}
			if minIndex == -1 || lines[i] < lines[minIndex] {
				minIndex = i
			}
		}

		// If all readers are exhausted, break
		if minIndex == -1 {
			break
		}

		// Write the smallest line to the output
		writer.WriteString(lines[minIndex] + "\n")

		// Read the next line from the file that had the smallest line
		lines[minIndex], errs[minIndex] = readLine(readers[minIndex])
		if errs[minIndex] != nil && errs[minIndex] != io.EOF {
			log.Fatalf("Failed to read line from run file: %v", errs[minIndex])
		}
	}

	writer.Flush()

	// Close all files
	for _, file := range files {
		file.Close()
	}
}

// readLine reads a single line from a buffered reader
func readLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	return strings.TrimSuffix(line, "\n"), err
}

// func main() {
// 	// Example usage
// 	db, err := pebble.Open("example.db", &pebble.Options{})
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer db.Close()

// 	externalSort(db, "sorted_output.txt", 5, 1000, 100, 10, "/tmp/run_")
// }
