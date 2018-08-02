package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/f110/fireworq-benchmarker/client"
	"github.com/f110/fireworq-benchmarker/worker"
	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
)

const (
	JobName = "benchmarker01"
)

var (
	Endpoint      = flag.String("endpoint", "http://localhost:8080", "fireworq endpoint")
	Concurrent    = flag.Int("concurrent", 10, "")
	NumberOfQueue = flag.Int("queue", 1000, "number of queues")
	FailureRate   = flag.Int("failure-rate", 0, "Failure rate (0-99)")
)

func main() {
	flag.Parse()
	if *FailureRate < 0 || *FailureRate > 99 {
		panic("Invalid failure rate")
	}

	w, err := worker.New(*FailureRate)
	if err != nil {
		panic(err)
	}
	go func() {
		fmt.Printf("Start Worker: %s\n", w.Addr)
		if err := w.Start(); err != nil {
			panic(err)
		}
	}()

	c := client.New("127.0.0.1:8080", *Concurrent)
	if err := c.CreateJobIfNotExist(JobName, 50); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	p := mpb.New(mpb.WithWaitGroup(&wg))
	enqueueBar := p.AddBar(int64(*NumberOfQueue),
		mpb.BarClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Enqueue", decor.WCSyncSpaceR),
			decor.OnComplete(decor.CountersNoUnit("%d / %d", decor.WCSyncWidth), "done!"),
			decor.OnComplete(decor.Percentage(decor.WCSyncSpace), ""),
		),
		mpb.AppendDecorators(

			decor.OnComplete(decor.AverageSpeed(0, "%.1f qps", decor.WCSyncSpace), ""),
		),
	)
	workerBar := p.AddBar(int64(*NumberOfQueue),
		mpb.BarClearOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Worker", decor.WCSyncSpaceR),
			decor.OnComplete(decor.CountersNoUnit("%d / %d", decor.WCSyncWidth), "done!"),
			decor.OnComplete(decor.Percentage(decor.WCSyncSpace), ""),
		),
		mpb.AppendDecorators(
			decor.OnComplete(decor.AverageSpeed(0, "%.1f qps", decor.WCSyncSpace), ""),
		),
	)
	perWorker := *NumberOfQueue / *Concurrent
	result := make(chan []int, *Concurrent)
	msg := make([]string, 0)
	sentIds := make([]int, 0, *NumberOfQueue)
	gotIds := make([]int, 0, *NumberOfQueue)
	t1 := time.Now()

	fmt.Printf("Concurrent: %d\n", *Concurrent)
	fmt.Printf("Total Jobs: %d\n", *NumberOfQueue)
	wg.Add(1)
	go func(bar *mpb.Bar) {
		defer wg.Done()

		for i := 0; i < *Concurrent; i++ {
			go func() {
				var p worker.Payload
				res := make([]int, perWorker)
				for k := 0; k < perWorker; k++ {
					r := rand.Int()
					p.Id = r
					c.Enqueue(JobName, &client.Job{Url: fmt.Sprintf("http://%s", w.Addr), Payload: &p, MaxRetries: 10})
					res[k] = r
					bar.Increment()
				}
				result <- res
			}()
		}

		for i := 0; i < *Concurrent; i++ {
			t := <-result
			sentIds = append(sentIds, t...)
		}
		t2 := time.Now().Sub(t1)
		msg = append(msg,
			"[Enqueue]\n",
			fmt.Sprintf("\tEnqueue Time: %.3f seconds\n", t2.Seconds()),
			fmt.Sprintf("\tEnqueue Throughput: %.3f qps\n", float64(*NumberOfQueue)/t2.Seconds()),
		)
	}(enqueueBar)

	wg.Add(1)
	go func(bar *mpb.Bar) {
		defer wg.Done()

		c := w.SucceededJobs()
	WatchLoop:
		for {
			select {
			case res := <-c:
				bar.Increment()
				gotIds = append(gotIds, res)
				if len(gotIds) == *NumberOfQueue {
					break WatchLoop
				}
			}
		}

		t2 := time.Now().Sub(t1)
		msg = append(msg,
			"[Worker]\n",
			fmt.Sprintf("\tWorker Time: %.2f seconds\n", t2.Seconds()),
			fmt.Sprintf("\tWorker Throughput: %.3f qps\n", float64(*NumberOfQueue)/t2.Seconds()),
			fmt.Sprintf("\tWorker Actual Throughput: %.3f qps\n", float64(w.Stat.Total())/t2.Seconds()),
		)
	}(workerBar)
	p.Wait()

	fmt.Println("----------")
	for _, m := range msg {
		fmt.Print(m)
	}
	success := true
	if len(sentIds) != len(gotIds) {
		fmt.Println("Got jobs differs from sent jobs.")
		success = false
	}
	sort.Ints(sentIds)
	sort.Ints(gotIds)
	for i := 0; i < len(sentIds); i++ {
		if sentIds[i] != gotIds[i] {
			fmt.Printf("Expected %d but got %d", sentIds[i], gotIds[i])
			success = false
			break
		}
	}
	if success == false {
		fmt.Println("Check Failed")
	}
}
