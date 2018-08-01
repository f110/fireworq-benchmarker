package worker

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"net"
	"net/http"
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

type Worker struct {
	Addr string

	mux      *http.ServeMux
	listener net.Listener
	server   *http.Server

	results chan int
}

func New() (*Worker, error) {
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		return nil, err
	}

	return &Worker{
		Addr:     listener.Addr().String(),
		listener: listener,
		mux:      http.NewServeMux(),
		results:  make(chan int, 0),
	}, nil
}

func (worker *Worker) Start() error {
	log.Printf("Start worker: %s", worker.Addr)
	s := &http.Server{
		Handler: worker,
	}
	worker.server = s

	return s.Serve(worker.listener)
}

func (worker *Worker) Stop(ctx context.Context) error {
	return worker.server.Shutdown(ctx)
}

func (worker *Worker) ArrivedJobs() chan int {
	return worker.results
}

func (worker *Worker) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	result := JobResult{Status: StatusSuccess}

	d := json.NewDecoder(req.Body)
	var payload Payload
	if err := d.Decode(&payload); err != nil {
		log.Print(err)
		result.Status = StatusFailure
		result.Message = err.Error()
		goto WriteResponse
	}

	if r := rand.Intn(100); r < 10 {
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
