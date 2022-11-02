package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"time"

	pgx "github.com/jackc/pgx/v5"
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
func main() {
	conn, err := pgx.Connect(context.Background(), "postgres://postgres:password@localhost:5432/homework")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	file, err := os.Open("../query_params.csv")
	if err != nil {
		log.Fatal(err)
	}
	parser := csv.NewReader(file)

	// NumWorkers := 5 // parse from cmd line
	// hostChannelMap := make(map[string]chan ([]string)) // send query params to NumWorkers goroutines
	// completionChannel := make(chan (QueryResult))      // receive query results from NumWorkers goroutines

	var totalQueries int64
	var queryTimes []int64

	startTime := time.Now()
	skip := true // One-time boolean flag to skip guaranteed header row in CSV
	for {        // Iterate over CSV contents to EOF
		row, err := parser.Read()
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

		// Fragile but we are guaranteed input format consistency in this case
		targetHost := row[0]
		start := row[1]
		end := row[2]

		// FIXME: Dispatch queries to host-adjacent goroutine here

		// fmt.Printf("Querying, host: %s, start: %v, end %v\n", targetHost, start, end)

		queryStartTime := time.Now()
		rows, err := conn.Query(context.Background(), "select host, ts, usage from cpu_usage where host = $1 and ts between $2 and $3", targetHost, start, end)
		if err != nil {
			fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
			os.Exit(1)
		}
		queryTimes = append(queryTimes, time.Since(queryStartTime).Nanoseconds())
		totalQueries++

		defer rows.Close()

		// Iterate over the sample time window we just requested
		maxCpu := float64(0.0)
		minCpu := float64(100.0)

		var host string
		var ts time.Time
		var usage float64

		var rowCount int
		for rows.Next() {
			rowCount++

			err = rows.Scan(&host, &ts, &usage)
			if err != nil {
				fmt.Printf("Scan error: %v", err)
				return
			}

			// Determine floor and ceiling of CPU usage range
			if usage < minCpu {
				minCpu = usage
			}
			if usage > maxCpu {
				maxCpu = usage
			}

			// fmt.Printf("Result, host %s, ts %v, usage %%%v, rowCount %d\n", host, ts, usage, rowCount)

		}

		// fmt.Printf("Window on host %s: rows %d, minCpu %%%v, maxCpu %%%v, qt len %d\n", host, rowCount, minCpu, maxCpu, len(queryTimes))

	}
	totalProcessTime := time.Since(startTime)

	// Taking names and kicking ass
	var totalQueryTimeInt int64
	for _, t := range queryTimes {
		totalQueryTimeInt += t
	}

	avgQueryTime := time.Duration(totalQueryTimeInt / totalQueries)

	// Used for diagnostic convenience
	totalQueryTime := time.Duration(totalQueryTimeInt)

	// Compute median, which requires ordered list, and gives us min and max for 'free'
	sort.Slice(queryTimes, func(i, j int) bool { return queryTimes[i] < queryTimes[j] })
	leftCenter := totalQueries / 2
	rightCenter := leftCenter + 1
	medianQueryTime := time.Duration((queryTimes[leftCenter] + queryTimes[rightCenter]) / 2)
	minQueryTime := time.Duration(queryTimes[0])
	maxQueryTime := time.Duration(queryTimes[totalQueries-1])

	fmt.Printf("totalProcessTime %v\ntotalQueryTime %v\nminQueryTime %v\nmaxQueryTime %v\nmedianQueryTime %v\navgQueryTime %v\n", totalProcessTime, totalQueryTime, minQueryTime, maxQueryTime, medianQueryTime, avgQueryTime)

	os.Exit(0)

}
