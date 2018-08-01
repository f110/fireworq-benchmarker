package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/f110/fireworq-benchmarker/client"
	"github.com/f110/fireworq-benchmarker/worker"
)

const (
	JobName       = "benchmarker01"
	NumberOfQueue = 1000
)

var (
	Endpoint   = flag.String("endpoint", "http://localhost:8080", "fireworq endpoint")
	Concurrent = flag.Int("concurrent", 10, "")
)

func main() {
	flag.Parse()

	w, err := worker.New()
	if err != nil {
		panic(err)
	}
	go func() {
		if err := w.Start(); err != nil {
			panic(err)
		}
	}()

	c := client.New("127.0.0.1:8080")
	if err := c.CreateJobIfNotExist(JobName, 50); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	perWorker := NumberOfQueue / *Concurrent
	result := make(chan []int, *Concurrent)
	t1 := time.Now()

	fmt.Printf("Concurrent: %d\n", *Concurrent)
	fmt.Printf("Total Jobs: %d\n", NumberOfQueue)
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < *Concurrent; i++ {
			go func() {
				var p worker.Payload
				res := make([]int, perWorker)
				for k := 0; k < perWorker; k++ {
					r := rand.Int()
					p.Id = r
					c.Enqueue(JobName, &client.Job{Url: fmt.Sprintf("http://%s", w.Addr), Payload: &p})
					res[k] = r
				}
				result <- res
			}()
		}

		total := make([]int, 0, NumberOfQueue)
		for i := 0; i < *Concurrent; i++ {
			t := <-result
			total = append(total, t...)
		}
		t2 := time.Now().Sub(t1)
		fmt.Printf("Enqueue Time: %.3f seconds\n", t2.Seconds())
		fmt.Printf("Enqueue Throughput: %.3f qps\n", NumberOfQueue/t2.Seconds())
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		c := w.ArrivedJobs()
		total := make([]int, 0, NumberOfQueue)
	WatchLoop:
		for {
			select {
			case res := <-c:
				total = append(total, res)
				if len(total) == NumberOfQueue {
					break WatchLoop
				}
				log.Print(len(total))
			}
		}

		t2 := time.Now().Sub(t1)
		fmt.Printf("Worker Time: %.2f seconds\n", t2.Seconds())
		fmt.Printf("Worker Throughput: %.3f qps\n", NumberOfQueue/t2.Seconds())
	}()
	wg.Wait()
}
