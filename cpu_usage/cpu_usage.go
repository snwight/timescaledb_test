package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	pgxpool "github.com/jackc/pgx/v5/pgxpool"
)

/*
Your tool should take the CSV row values hostname, start time, end time and use them to generate a
SQL query for each row that returns the max cpu usage and min cpu usage of the given hostname for
every minute in the time range specified by the start time and end time
- # of queries processed,
- total processing time across all queries,
- the minimum query time (for a single query),
- the median query time,
- the average query time,
- and the maximum query time.
*/

type QueryStats struct {
	queryTime int64
	minCpu    float64
	maxCpu    float64
}

type QueryStatReport struct {
	totalProcessTime time.Duration
	totalQueryTime   time.Duration
	minQueryTime     time.Duration
	maxQueryTime     time.Duration
	medianQueryTime  time.Duration
	avgQueryTime     time.Duration
}

func main() {
	pool, err := pgxpool.New(context.Background(), "postgres://postgres:password@localhost:5432/homework")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	file, err := os.Open("../query_params.csv")
	if err != nil {
		log.Fatal(err)
	}
	parser := csv.NewReader(file)

	// FIXME parse from cmd line
	numWorkers := 5

	// Dispatch and collection channel
	completionChannel := make(chan (QueryStats)) // receive query results from NumWorkers goroutines
	terminationChannel := make(chan bool)
	reportChan := make(chan QueryStatReport)
	hostChannelMap := spawnWorkers(pool, numWorkers, completionChannel, terminationChannel)

	// Completed tasks will be aggregated in this waiting processs
	go completionTasks(time.Now(), completionChannel, terminationChannel, reportChan, numWorkers)

	skip := true // One-time boolean flag to skip guaranteed header row in CSV
	for {        // Iterate over CSV contents to EOF
		csvRow, err := parser.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if skip {
			skip = false
			continue
		}

		// Dispatch queries to host-adjacent goroutines here via chan msg
		ch := mapQuery(csvRow[0], numWorkers, hostChannelMap)
		ch <- csvRow

	}

	// Signal cleanup time to workers
	for _, m := range hostChannelMap {
		m <- nil
	}

	// Await final report from completionTasks
	queryReport := <-reportChan
	fmt.Printf("totalProcessTime %v\ntotalQueryTime %v\nminQueryTime %v\nmaxQueryTime %v\nmedianQueryTime %v\navgQueryTime %v\n",
		queryReport.totalProcessTime, queryReport.totalQueryTime, queryReport.minQueryTime, queryReport.maxQueryTime, queryReport.medianQueryTime, queryReport.avgQueryTime)

}

func spawnWorkers(pool *pgxpool.Pool, numWorkers int, completionChannel chan (QueryStats), terminationChannel chan bool) map[int]chan ([]string) {

	// Create a receiving channel for each goroutine worker and spin them up
	hostChannelMap := make(map[int]chan ([]string))
	for i := 0; i < numWorkers; i++ {

		// Map key is effectively the channel label
		hostChannelMap[i] = make(chan ([]string))

		// Send forth the headless worker to listen on channel
		// fmt.Printf("Creating goroutine with host map %+v\n", hostChannelMap[i])
		go func(parameterChannel chan ([]string)) {

			for {

				csvRow := <-parameterChannel
				// A nil packet is our termination flag, we forward the msg to originator and kill this goroutine
				if csvRow == nil {
					terminationChannel <- true
					return
				}

				// I'll check for errors when I'm dead
				res := processQuery(pool, csvRow)
				// fmt.Printf("Result, worker: %v, %v\n", parameterChannel, res)

				completionChannel <- res
			}

		}(hostChannelMap[i])
	}
	return hostChannelMap
}

func mapQuery(host string, numWorkers int, channelMap map[int]chan ([]string)) chan []string {

	// FIXME ceci n'est pas un hash
	var idx int
	s := strings.SplitAfter(host, "_")
	fmt.Sscanf(s[1], "%d", &idx)
	hashMod := idx % numWorkers

	// fmt.Printf("Mapping host %s to idx %d numworkers %d hashMod %v\n", host, idx, numWorkers, hashMod)

	return channelMap[hashMod]
}

func processQuery(pool *pgxpool.Pool, row []string) QueryStats {

	// Fragile but we are guaranteed input format consistency in this case
	targetHost := row[0]
	start := row[1]
	end := row[2]

	// fmt.Printf("Querying, host: %s, start: %v, end %v\n", targetHost, start, end)

	queryStartTime := time.Now()
	rows, err := pool.Query(context.Background(), "select ts, usage from cpu_usage where host = $1 and ts between $2 and $3", targetHost, start, end)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v, row: %v\n", err, row)
		os.Exit(1)
	}
	queryTime := time.Since(queryStartTime).Nanoseconds()

	defer rows.Close()

	var ts time.Time
	var usage float64
	maxCpu := float64(0.0)
	minCpu := float64(100.0)

	for rows.Next() {

		err = rows.Scan(&ts, &usage)
		if err != nil {
			fmt.Printf("Scan error: %v", err)
			os.Exit(1)
		}

		// Determine floor and ceiling of CPU usage range
		if usage < minCpu {
			minCpu = usage
		}
		if usage > maxCpu {
			maxCpu = usage
		}

	}

	// Returned values
	return QueryStats{
		queryTime: queryTime,
		minCpu:    minCpu,
		maxCpu:    maxCpu,
	}

}

func completionTasks(startTime time.Time, completionChannel chan (QueryStats), terminationChannel chan bool, reportChan chan QueryStatReport, liveWorkers int) {

	// Kicking ass and taking names
	var totalQueries int64
	var totalQueryTimeInt int64
	var minCpu float64 = 100
	var maxCpu float64
	var queryTimes []int64

	stop := false
	for {

		select {
		case queryStats := <-completionChannel:

			// fmt.Println("queryStats: ", queryStats)
			totalQueries++
			totalQueryTimeInt += queryStats.queryTime
			queryTimes = append(queryTimes, queryStats.queryTime)
			if queryStats.minCpu < minCpu {
				minCpu = queryStats.minCpu
			}
			if queryStats.maxCpu > maxCpu {
				maxCpu = queryStats.maxCpu
			}

		case <-terminationChannel:

			liveWorkers--
			if liveWorkers == 0 {
				stop = true
			}

		}

		if stop {
			break
		}

	}

	totalProcessTime := time.Since(startTime)
	avgQueryTime := time.Duration(totalQueryTimeInt / totalQueries)
	totalQueryTime := time.Duration(totalQueryTimeInt)

	// Compute median, which requires ordered list, and gives us min and max for 'free'
	sort.Slice(queryTimes, func(i, j int) bool { return queryTimes[i] < queryTimes[j] })
	leftCenter := totalQueries / 2
	rightCenter := leftCenter + 1
	medianQueryTime := time.Duration((queryTimes[leftCenter] + queryTimes[rightCenter]) / 2)
	minQueryTime := time.Duration(queryTimes[0])
	maxQueryTime := time.Duration(queryTimes[totalQueries-1])

	queryStatReport := QueryStatReport{
		totalProcessTime,
		totalQueryTime,
		minQueryTime,
		maxQueryTime,
		medianQueryTime,
		avgQueryTime,
	}
	reportChan <- queryStatReport

}
