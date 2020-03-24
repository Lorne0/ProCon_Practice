package procon

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
)

// Consumer consumes from broker
type Consumer struct {
	lastOffsets map[string]int
}

type consumerResponse struct {
	Data   interface{} `json:"data"`
	Offset int         `json:"offset"`
}

// Consume from the broker by http request
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

	res, err := http.Get(addr)
	if err != nil {
		return nil, errors.New("cons.Consume: http.Get error")
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, errors.New("cons.Consume: ioutil.ReadAll error")
	}

	var data consumerResponse
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, errors.New("cons.Consume: json.Unmarshal error")
	}

	c.lastOffsets[topic] = data.Offset

	return data.Data, nil

}
