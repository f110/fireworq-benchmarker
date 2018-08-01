package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

type Client struct {
	Host string
	url  *url.URL
}

type Job struct {
	Id         int         `json:"id,omitempty"`
	QueueName  string      `json:"queue_name,omitempty"`
	Category   string      `json:"category,omitempty"`
	Url        string      `json:"url"`
	Payload    interface{} `json:"payload"`
	RunAfter   int         `json:"run_after"`
	MaxRetries int         `json:"max_retries"`
	RetryDelay int         `json:"retry_delay"`
	Timeout    int         `json:"timeout"`
}

type Queue struct {
	PollingInterval int `json:"polling_interval"`
	MaxWorkers      int `json:"max_workers"`
}

func New(host string) *Client {
	u, err := url.Parse(fmt.Sprintf("http://%s", host))
	if err != nil {
		return nil
	}

	return &Client{Host: host, url: u}
}

func (c *Client) CreateJob(name string, maxWorkers int) error {
	c.url.Path = fmt.Sprintf("/queue/%s", name)
	req, err := http.NewRequest(http.MethodGet, c.url.String(), nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		if err := c.createQueue(name, &Queue{PollingInterval: 100, MaxWorkers: maxWorkers}); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) Enqueue(name string, j *Job) (*Job, error) {
	c.url.Path = fmt.Sprintf("/job/%s", name)

	var buf bytes.Buffer
	e := json.NewEncoder(&buf)
	if err := e.Encode(j); err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodPost, c.url.String(), &buf)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var enqueuedJob Job
	d := json.NewDecoder(res.Body)
	if err := d.Decode(&enqueuedJob); err != nil {
		return nil, err
	}

	return &enqueuedJob, nil
}

func (c *Client) createQueue(name string, q *Queue) error {
	c.url.Path = fmt.Sprintf("/queue/%s", name)

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(q); err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPut, c.url.String(), &buf)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusBadRequest {
		return errors.New("failed create queue")
	}

	return nil
}

func (c *Client) createRouting(name string) {}
