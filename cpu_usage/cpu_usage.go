package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pgx "github.com/jackc/pgx/v5"
)

func main() {
	file, err := os.Open("../query_params.csv")
	if err != nil {
		log.Fatal(err)
	}

	conn, err := pgx.Connect(context.Background(), "postgres://postgres:password@localhost:5432/homework")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	parser := csv.NewReader(file)

	skip := true
	for {
		record, err := parser.Read()
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

		fmt.Println("Querying: ", record)

		var host string
		var ts time.Time
		var usage float64

		start, err := time.Parse("2006-1-2 15:04:05", "2017-01-01 00:03:40")
		if err != nil {
			fmt.Fprintf(os.Stderr, "timeParse failed: %v\n", err)
			os.Exit(1)
		}

		end, err := time.Parse("2006-1-2 15:04:05", "2017-01-01 00:04:40")
		if err != nil {
			fmt.Fprintf(os.Stderr, "timeParse failed: %v\n", err)
			os.Exit(1)
		}

		err = conn.QueryRow(context.Background(), "select host, ts, usage from cpu_usage where ts between $1 and $2", start, end).Scan(&host, &ts, &usage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Println("Result: ", host, ts, usage)

	}

}
