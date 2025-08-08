package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type StationStats struct {
	Count int64
	Min   float64
	Max   float64
	Sum   float64
}

func main() {
	if err := MustRun(os.Args, os.Stdout, os.Stderr); err != nil {
		fmt.Fprint(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func MustRun(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet(args[0], flag.ExitOnError)
	fWorkers := flags.Int("w", 0, "workers (default: num of logical CPUs)")
	fFile := flags.String("f", "data.txt", "path to data txt file")
	fProfileMem := flags.String("profmem", "", "generate memory profile file")
	fProfileCPU := flags.String("profcpu", "", "generate CPU profile file")
	fGenerate := flags.Bool("generate", false, "generate the data file")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}

	if *fWorkers == 0 {
		*fWorkers = runtime.NumCPU()
	}

	if *fProfileCPU != "" {
		f, err := os.Create(*fProfileCPU)
		if err != nil {
			return fmt.Errorf("creating CPU profile file: %v", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			return fmt.Errorf("starting CPU profiler: %v", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
		}()
	}
	if *fProfileMem != "" {
		f, err := os.Create("mem.prof")
		if err != nil {
			return fmt.Errorf("creating memory profile file: %v", err)
		}
		defer func() { _ = f.Close() }()

		runtime.GC() // Force GC to get up-to-date mem stats

		if err := pprof.WriteHeapProfile(f); err != nil {
			return fmt.Errorf("writing memory profile: %v", err)
		}
	}

	_, _ = fmt.Fprintln(stderr, "Billion row challenge go version")
	_, _ = fmt.Fprintf(stderr, "Using %d parallel workers\n", *fWorkers)

	if *fGenerate {
		return generate(*fFile)
	}

	if _, err := os.Stat(*fFile); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf(
			"file %s does not exist, generate data first with -generate", *fFile,
		)
	}

	file, err := os.Open(*fFile)
	if err != nil {
		return fmt.Errorf("opening file: %v", err)
	}
	defer func() { _ = file.Close() }()

	start := time.Now()

	data, cleanup, err := mmapFile(file)
	if err != nil {
		return fmt.Errorf("memory-mapping file: %v", err)
	}
	defer cleanup()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("getting file info: %v", err)
	}
	fileSize := fileInfo.Size()

	chunks := calculateChunks(data, fileSize, *fWorkers)
	results := make([]map[string]*StationStats, *fWorkers)

	var errg errgroup.Group
	for i := range *fWorkers {
		errg.Go(func() (err error) {
			results[i], err = processChunk(data, chunks[i])
			return err
		})
	}
	if err := errg.Wait(); err != nil {
		return err
	}

	finalStats := make(map[string]StationStats, 10000)
	for _, workerResult := range results {
		for station, stats := range workerResult {
			if existing, ok := finalStats[station]; ok {
				existing.Min = min(existing.Min, stats.Min)
				existing.Max = max(existing.Max, stats.Max)
				existing.Sum += stats.Sum
				existing.Count += stats.Count
			} else {
				finalStats[station] = *stats
			}
		}
	}

	duration := time.Since(start)

	printResults(stdout, finalStats)
	printResultStats(stderr, duration, fileSize)
	return nil
}

func calculateChunks(data string, fileSize int64, numWorkers int) [][2]int64 {
	chunks := make([][2]int64, numWorkers)
	chunkSize := fileSize / int64(numWorkers)

	var currentPos int64 = 0
	for i := range numWorkers {
		start := currentPos
		end := start + chunkSize
		if end >= fileSize {
			end = fileSize
		} else {
			newlineIndex := strings.IndexByte(data[end:], '\n')
			if newlineIndex != -1 {
				end += int64(newlineIndex) + 1
			} else {
				end = fileSize
			}
		}

		chunks[i] = [2]int64{start, end}
		currentPos = end
	}

	chunks[numWorkers-1][1] = fileSize

	return chunks
}

func readNameUntilSemicolon(input string) string {
	s := input
	var offset int

	for len(s) > 7 {
		if s[0] == ';' {
			goto END
		}
		if s[1] == ';' {
			offset++
			goto END
		}
		if s[2] == ';' {
			offset += 2
			goto END
		}
		if s[3] == ';' {
			offset += 3
			goto END
		}
		if s[4] == ';' {
			offset += 4
			goto END
		}
		if s[5] == ';' {
			offset += 5
			goto END
		}
		if s[6] == ';' {
			offset += 6
			goto END
		}
		if s[7] == ';' {
			offset += 7
			goto END
		}

		s = s[8:]
		offset += 8
	}
	// tail
	for i := range len(s) {
		if s[i] == ';' {
			offset += i
			goto END
		}
	}
	// no semicolon found; return whole input
	return input
END:
	return input[:offset]
}

func processChunk(data string, chunk [2]int64) (map[string]*StationStats, error) {
	stats := make(map[string]*StationStats, 10_000)
	i := chunk[0]
	end := chunk[1]

	for i < end {
		// slice of remaining data
		remaining := data[i:end]

		// extract name
		name := readNameUntilSemicolon(remaining)
		if len(name) == len(remaining) {
			// no semicolon found, malformed
			break
		}
		i += int64(len(name)) + 1 // skip name + ';'

		// extract temperature until '\n'
		start := i
		for i < end && data[i] != '\n' {
			i++
		}

		temp, ok := parseTemp(data[start:i])
		if !ok {
			return nil, fmt.Errorf("malformed number: %q", data[start:i])
		}

		if i < end && data[i] == '\n' {
			i++
		}

		if s, ok := stats[name]; ok {
			s.Min = min(s.Min, temp)
			s.Max = max(s.Max, temp)
			s.Sum += temp
			s.Count++
		} else {
			stats[name] = &StationStats{
				Min:   temp,
				Max:   temp,
				Sum:   temp,
				Count: 1,
			}
		}
	}

	return stats, nil
}

func printResults(w io.Writer, stats map[string]StationStats) {
	stationNames := make([]string, 0, len(stats))
	for name := range stats {
		stationNames = append(stationNames, name)
	}
	sort.Strings(stationNames)

	_, _ = fmt.Fprint(w, "{")
	for i, name := range stationNames {
		s := stats[name]
		avg := float64(s.Sum) / float64(s.Count)
		_, _ = fmt.Fprintf(w, "%s=%.2f/%.2f/%.2f", name, s.Min, avg, s.Max)
		if i < len(stationNames)-1 {
			_, _ = fmt.Fprint(w, ", ")
		}
	}
	_, _ = fmt.Fprint(w, "}\n")
}

func printResultStats(w io.Writer, duration time.Duration, fileSize int64) {
	_, _ = fmt.Fprintf(w, "\nRESULTS\n")
	_, _ = fmt.Fprintf(w, "Total Time: %v\n", duration)
	rowsPerSecond := float64(1_000_000_000) / duration.Seconds()
	gbPerSecond := float64(fileSize) / (1024 * 1024 * 1024) / duration.Seconds()
	_, _ = fmt.Fprintf(w, "Speed: %.2f million rows/second\n", rowsPerSecond/1_000_000)
	_, _ = fmt.Fprintf(w, "I/O Rate: %.2f GB/second\n", gbPerSecond)
}

func generate(file string) error {
	generator := NewBillionRowGenerator()

	if err := generator.LoadStations("weather_stations.csv"); err != nil {
		return fmt.Errorf("loading stations: %v", err)
	}

	if _, err := os.Stat(file); err == nil {
		fmt.Printf("File %s already exists. Overwrite? (y/N): ", file)
		var response string
		_, _ = fmt.Scanln(&response)
		if response != "y" && response != "Y" {
			fmt.Println("Generation cancelled")
			return nil
		}
	}

	totalStart := time.Now()
	if err := generator.Generate(file); err != nil {
		return fmt.Errorf("generating data: %v", err)
	}
	totalDuration := time.Since(totalStart)

	fmt.Printf("\nGENERATION COMPLETE\n")
	fmt.Printf("Total time: %v\n", totalDuration)
	return nil
}

func parseTemp(b string) (float64, bool) {
	if len(b) < 4 { // min "0.00"
		return 0, false
	}
	i := 0
	neg := false
	if b[0] == '-' {
		neg = true
		i = 1
		if len(b)-i < 4 { // need at least D.DD
			return 0, false
		}
	}

	// Detect dot position: i+1, i+2, or i+3
	// and ensure we have exactly two digits after it.
	var intv int32
	switch {
	case i+3 < len(b) && b[i+1] == '.': // D.DD
		d0 := b[i+0] - '0'
		d1 := b[i+2] - '0'
		d2 := b[i+3] - '0'
		if d0 > 9 || d1 > 9 || d2 > 9 {
			return 0, false
		}
		intv = int32(d0)
		frac := int32(d1)*10 + int32(d2)
		v := intv*100 + frac
		if neg {
			v = -v
		}
		return float64(v) * 0.01, true

	case i+4 < len(b) && b[i+2] == '.': // DD.DD
		d0 := b[i+0] - '0'
		d1 := b[i+1] - '0'
		d2 := b[i+3] - '0'
		d3 := b[i+4] - '0'
		if d0 > 9 || d1 > 9 || d2 > 9 || d3 > 9 {
			return 0, false
		}
		intv = int32(d0)*10 + int32(d1)
		frac := int32(d2)*10 + int32(d3)
		v := intv*100 + frac
		if neg {
			v = -v
		}
		return float64(v) * 0.01, true

	case i+5 < len(b) && b[i+3] == '.': // DDD.DD (e.g. 100.00)
		d0 := b[i+0] - '0'
		d1 := b[i+1] - '0'
		d2 := b[i+2] - '0'
		d3 := b[i+4] - '0'
		d4 := b[i+5] - '0'
		if d0 > 9 || d1 > 9 || d2 > 9 || d3 > 9 || d4 > 9 {
			return 0, false
		}
		intv = int32(d0)*100 + int32(d1)*10 + int32(d2)
		frac := int32(d3)*10 + int32(d4)
		v := intv*100 + frac
		if neg {
			v = -v
		}
		return float64(v) * 0.01, true
	}

	return 0, false
}
