package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

type DataGenerator interface {
	LoadStations(filename string) error
	Generate(outputFilename string) error
	GetStationCount() int
}

type BillionRowGenerator struct {
	stations []string
}

const (
	totalRows = 1_000_000_000
	chunkSize = 5_000_000
)

func NewBillionRowGenerator() *BillionRowGenerator {
	return &BillionRowGenerator{}
}

func (g *BillionRowGenerator) LoadStations(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("error opening stations file: %v", err)
	}
	defer func() { _ = file.Close() }()

	var stations []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Split(line, ";")
		if len(parts) > 0 {
			stations = append(stations, parts[0])
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading stations file: %v", err)
	}

	g.stations = stations
	fmt.Printf("Loaded %d weather stations\n", len(stations))
	return nil
}

func (g *BillionRowGenerator) GetStationCount() int {
	return len(g.stations)
}

func (g *BillionRowGenerator) generateChunk(numRows int, seed int64, output chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	rng := rand.New(rand.NewSource(seed))
	var builder strings.Builder
	builder.Grow(numRows * 40) // Pre-allocate space

	for i := 0; i < numRows; i++ {
		station := g.stations[rng.Intn(len(g.stations))]
		temp := -100.0 + rng.Float64()*200.0 // -100 to 100
		builder.WriteString(fmt.Sprintf("%s;%.2f\n", station, temp))
	}

	output <- builder.String()
}

func (g *BillionRowGenerator) Generate(outputFilename string) error {
	if len(g.stations) == 0 {
		return fmt.Errorf("no stations loaded - call LoadStations() first")
	}

	numChunks := totalRows / chunkSize
	numWorkers := runtime.NumCPU()

	fmt.Printf("Generating %d rows using %d workers (%d chunks of %dM rows)\n",
		totalRows, numWorkers, numChunks, chunkSize/1_000_000)

	file, err := os.Create(outputFilename)
	if err != nil {
		return fmt.Errorf("error creating output file: %v", err)
	}
	defer func() { _ = file.Close() }()

	writer := bufio.NewWriterSize(file, 64*1024*1024) // 64MB buffer
	defer func() { _ = writer.Flush() }()

	output := make(chan string, numWorkers*2)
	var wg sync.WaitGroup

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		chunksWritten := 0
		for chunk := range output {
			_, _ = writer.WriteString(chunk)
			chunksWritten++
			if chunksWritten%10 == 0 {
				progress := float64(chunksWritten) / float64(numChunks) * 100
				fmt.Printf("Generated %d/%d chunks (%.1f%%)\n",
					chunksWritten, numChunks, progress)
			}
		}
	}()

	semaphore := make(chan struct{}, numWorkers)
	startTime := time.Now()

	wg.Add(numChunks)
	for i := range numChunks {
		semaphore <- struct{}{}
		go func(chunkId int) {
			defer func() { <-semaphore }()
			g.generateChunk(chunkSize, int64(chunkId), output, &wg)
		}(i)
	}

	wg.Wait()
	close(output)

	duration := time.Since(startTime)
	fmt.Printf("Generation complete in %v\n", duration)
	fmt.Printf("Generation speed: %.1f million rows/second\n",
		float64(totalRows)/duration.Seconds()/1_000_000)

	fileInfo, _ := file.Stat()
	fileSizeGB := float64(fileInfo.Size()) / (1024 * 1024 * 1024)
	fmt.Printf("Created file: %s (%.1f GB)\n", outputFilename, fileSizeGB)
	fmt.Printf("Write speed: %.1f GB/second\n", fileSizeGB/duration.Seconds())

	return nil
}
