package main

import (
	"flag"

	"github.com/f110/fireworq-benchmarker/worker"
)

var (
	Endpoint = flag.String("endpoint", "http://localhost:8080", "fireworq endpoint")
)

func main() {
	w, err := worker.New()
	if err != nil {
		panic(err)
	}
	if err := w.Start(); err != nil {
		panic(err)
	}
}
