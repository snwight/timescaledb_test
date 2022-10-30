package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	file, err := os.Open("query_params.csv")
	if err != nil {
		log.Fatal(err)
	}

	parser := csv.NewReader(file)

	for {
		record, err := parser.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(record)
	}

}
