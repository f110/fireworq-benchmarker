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

type Routing struct {
	QueueName   string `json:"queue_name"`
	JobCategory string `json:"job_category,omitempty"`
}

func New(host string, concurrent int) *Client {
	u, err := url.Parse(fmt.Sprintf("http://%s", host))
	if err != nil {
		return nil
	}
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = concurrent

	return &Client{Host: host, url: u}
}

func (c *Client) CreateJobIfNotExist(name string, maxWorkers int) error {
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

	c.url.Path = fmt.Sprintf("/routing/%s", name)
	req, err = http.NewRequest(http.MethodGet, c.url.String(), nil)
	if err != nil {
		return err
	}
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusNotFound {
		if err := c.createRouting(name); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) Enqueue(name string, j *Job) (*Job, error) {
	var result Job
	if err := c.call(http.MethodPost, fmt.Sprintf("/job/%s", name), j, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *Client) createQueue(name string, q *Queue) error {
	return c.call(http.MethodPut, fmt.Sprintf("/queue/%s", name), q, &Queue{})
}

func (c *Client) createRouting(name string) error {
	return c.call(http.MethodPut, fmt.Sprintf("/routing/%s", name), &Routing{QueueName: name}, &Routing{})
}

func (c *Client) call(method string, path string, body interface{}, result interface{}) error {
	c.url.Path = path

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return err
	}

	req, err := http.NewRequest(method, c.url.String(), &buf)
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

	if err := json.NewDecoder(res.Body).Decode(result); err != nil {
		return err
	}

	return nil
}
