package procon

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
)

// Producer produces data to broker
type Producer struct {
	Topics []string
}

// NewProducer creates producer
func NewProducer() *Producer {
	return &Producer{Topics: make([]string, 0)}
}

// Produce to broker
func (p *Producer) Produce(port string, topic string, data interface{}) error {
	p.Topics = append(p.Topics, topic)

	if string(port[0]) != ":" {
		port = ":" + port
	}
	addr := "http://127.0.0.1" + port + "/producer"

	jsonData := map[string]interface{}{"topic": topic, "data": data}
	jsonStr, err := json.Marshal(jsonData)
	if err != nil {
		return errors.New("prod.Produce: json.Marshal error")
	}
	req, err := http.NewRequest("POST", addr, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.New("prod.Produce: http.DefualtClient.Do error")
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.New("cons.Consume: ioutil.ReadAll error")
	}

	if res.StatusCode != 200 {
		return errors.New(string(body))
	}

	return nil
}
