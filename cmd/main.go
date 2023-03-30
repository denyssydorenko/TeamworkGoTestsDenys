package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	ci "teamwork/customerimporter"
)

const (
	INPUT_FILE_NAME      = "../data/customers.csv"
	OUTPUT_FILE_NAME_PER = "../output/personas.csv"
	OUTPUT_FILE_NAME_DOM = "../output/domains.csv"
)

var sortedDom = []string{}

func main() {

	// wait for all workers to finish up before exit
	wg := ci.WaitGroup()
	defer wg.Wait()

	os.Mkdir("../output", os.ModePerm)

	file, err := os.Open(INPUT_FILE_NAME)
	if err != nil {
		fmt.Println(err)
		return
	}

	r := csv.NewReader(file)

	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				ci.CloseWorkers()
				fmt.Println("Persons ordered by email domain printed in " + OUTPUT_FILE_NAME_PER)
				fmt.Println("Domains count printed in " + OUTPUT_FILE_NAME_DOM)
				// wait for all workers to finish up before writing to output files
				wg.Wait()
				doms := ci.Domains()
				for k, _ := range doms {
					sortedDom = ci.Insert(sortedDom, k)
				}
				out, err := os.Create(OUTPUT_FILE_NAME_PER)
				if err != nil {
					fmt.Println(err)
				}
				defer out.Close()
				ci.WriteCSVOutput(out, sortedDom)

				dom, err := os.Create(OUTPUT_FILE_NAME_DOM)
				if err != nil {
					fmt.Println(err)
				}
				defer out.Close()
				ci.WriteCSVDomainsCount(dom, sortedDom)
				return
			}
			log.Fatal(err) // will provoke panic
		}
		ci.Process(rec)
	}
}
