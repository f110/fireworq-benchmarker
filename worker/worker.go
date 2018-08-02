package worker

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
)

const (
	StatusSuccess          = "success"
	StatusFailure          = "failure"
	StatusPermanentFailure = "permanent-failure"
)

type JobResult struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

type Payload struct {
	Id int `json:"id"`
}

type Stat struct {
	sync.RWMutex
	total   int
	failure int
	error   int
}

type Worker struct {
	Addr string

	failureRate int
	mux         *http.ServeMux
	listener    net.Listener
	server      *http.Server

	results chan int
	Stat    *Stat
}

func New(failureRate int) (*Worker, error) {
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}

	return &Worker{
		Addr:        listener.Addr().String(),
		failureRate: failureRate,
		listener:    listener,
		mux:         http.NewServeMux(),
		results:     make(chan int, 0),
		Stat:        &Stat{},
	}, nil
}

func (worker *Worker) Start() error {
	s := &http.Server{
		Handler: worker,
	}
	worker.server = s

	return s.Serve(worker.listener)
}

func (worker *Worker) Stop(ctx context.Context) error {
	return worker.server.Shutdown(ctx)
}

func (worker *Worker) SucceededJobs() chan int {
	return worker.results
}

func (worker *Worker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	result := JobResult{Status: StatusSuccess}

	d := json.NewDecoder(req.Body)
	var payload Payload
	if err := d.Decode(&payload); err != nil {
		log.Print(err)
		worker.Stat.ErrorOccurred()
		result.Status = StatusFailure
		result.Message = err.Error()
		goto WriteResponse
	}

	worker.Stat.ArriveJob()

	if r := rand.Intn(100); r < worker.failureRate {
		worker.Stat.Fail()
		result.Status = StatusFailure
		result.Message = "random failure"
		goto WriteResponse
	}

WriteResponse:
	switch result.Status {
	case StatusSuccess:
		worker.results <- payload.Id
	}
	w.WriteHeader(http.StatusOK)
	e := json.NewEncoder(w)
	if err := e.Encode(&result); err != nil {
		log.Print(err)
		return
	}
}

func (s *Stat) Total() int {
	s.RLock()
	defer s.RUnlock()
	return s.total
}

func (s *Stat) Failure() int {
	s.RLock()
	defer s.RUnlock()
	return s.failure
}

func (s *Stat) Error() int {
	s.RLock()
	defer s.RUnlock()
	return s.failure
}

func (s *Stat) ArriveJob() {
	s.Lock()
	defer s.Unlock()
	s.total++
}

func (s *Stat) Fail() {
	s.Lock()
	defer s.Unlock()
	s.failure++
}

func (s *Stat) ErrorOccurred() {
	s.Lock()
	defer s.Unlock()
	s.error++
}
