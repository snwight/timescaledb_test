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

	var host string
	var ts time.Time
	var usage float64

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

		start := record[1]
		end := record[2]

		fmt.Printf("Querying, host: %s, start: %v, end %v\n", record[0], start, end)

		err = conn.QueryRow(context.Background(), "select host, ts, usage from cpu_usage where ts between $1 and $2", start, end).Scan(&host, &ts, &usage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("Result, host %s, ts %v, usage %%%v\n", host, ts, usage)

	}

	os.Exit(0)

}
