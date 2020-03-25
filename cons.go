package procon

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
)

// Consumer consumes from broker
type Consumer struct {
	lastOffsets map[string]int
}

type consumerResponse struct {
	Data   interface{} `json:"data"`
	Offset int         `json:"offset"`
}

// NewConsumer creates consumer
func NewConsumer() *Consumer {
	return &Consumer{lastOffsets: make(map[string]int)}
}

// Consume from the broker by http request
// set s == -1 if you want to use last offset
// but if s == -1 and it's the first time to consume, s would be 0
// set e == -1 if you want to consume to the latest data
func (c *Consumer) Consume(p string, topic string, s int, e int) (interface{}, error) {
	if string(p[0]) != ":" {
		p = ":" + p
	}
	addr := "http://127.0.0.1" + p + "/consumer"

	if s == -1 {
		if l, ok := c.lastOffsets[topic]; ok {
			s = l
		}
	}
	if s == -1 { // s still be -1 since it's first time to see the topic
		s = 0
	}

	// Make query and request
	req, err := http.NewRequest("GET", addr, nil)
	if err != nil {
		return nil, errors.New("cons.Consume http.NewRequest error")
	}
	q := req.URL.Query()
	q.Add("topic", topic)
	q.Add("s_offset", strconv.Itoa(s))
	q.Add("e_offset", strconv.Itoa(e))
	req.URL.RawQuery = q.Encode()

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.New("cons.Consume: http.DefualtClient.Do error")
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.New("cons.Consume: ioutil.ReadAll error")
	}

	if res.StatusCode != 200 {
		return nil, errors.New(string(body))
	}

	var data consumerResponse
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, errors.New("cons.Consume: json.Unmarshal error")
	}

	c.lastOffsets[topic] = data.Offset

	return data.Data, nil

}
