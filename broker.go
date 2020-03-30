package procon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
)

// Topic defines struct of topic
type Topic struct {
	Name string
	MQ   []interface{}
	mu   *sync.RWMutex
}

// Broker defines struct of broker
type Broker struct {
	topics map[string]*Topic
	mu     *sync.RWMutex
}

type brokerServer struct {
	broker *Broker
	mux    *http.ServeMux
}

type postData struct {
	Topic string      `json:"topic"`
	Data  interface{} `json:"data"`
}

func (bs *brokerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	bs.mux.ServeHTTP(w, r)
}

func (b *Broker) newTopic(name string) {
	mu := &sync.RWMutex{}
	t := &Topic{
		Name: name,
		MQ:   make([]interface{}, 0),
		mu:   mu,
	}
	// Use mutex lock to lock writing
	// b.mu.Lock()
	// defer b.mu.Unlock()
	// block outside...
	b.topics[name] = t
}

func (b *Broker) readTopic(topic string, s int, e int) ([]interface{}, int, error) {
	// Add broker lock to check the topic exist
	b.mu.RLock()
	if _, ok := b.topics[topic]; !ok {
		b.mu.RUnlock()
		return nil, -1, errors.New("the topic is not in broker")
	}
	b.mu.RUnlock()

	// lock the topic
	b.topics[topic].mu.RLock()
	defer b.topics[topic].mu.RUnlock()
	mq := b.topics[topic].MQ

	if e == -1 {
		e = len(mq)
	}

	if s >= len(mq) {
		return nil, -1, errors.New("start offset can't be larger than topic size")
	}

	if s >= e {
		return nil, -1, errors.New("start offset should be less than end offset")
	}

	if e > len(mq) {
		return nil, -1, errors.New("end offset should be less than topic size")
	}

	cmq := make([]interface{}, e-s)
	copy(cmq, mq[s:e])
	return cmq, e, nil
}

func (bs *brokerServer) consumerHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/consumer" {
		http.Error(w, "404 Not Found.", http.StatusNotFound)
		return
	}

	if r.Method != "GET" {
		http.Error(w, "405 Method Not Allowed.", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "400 Bad Request: topic is required.", http.StatusBadRequest)
		return
	}

	// start offset
	s, err := strconv.Atoi(r.URL.Query().Get("s_offset"))
	if err != nil {
		http.Error(w, "400 Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}
	// end offset
	e, err := strconv.Atoi(r.URL.Query().Get("e_offset"))
	if err != nil {
		http.Error(w, "400 Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}

	data, offset, err := bs.broker.readTopic(topic, s, e)
	if err != nil {
		http.Error(w, "400 Bad Request: "+err.Error(), http.StatusBadRequest)
		return
	}
	result := map[string]interface{}{"data": data, "offset": offset}

	js, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		http.Error(w, "500 Internal Server Error: json marshal error.", http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, string(js))

}

func (b *Broker) writeTopic(data postData) int {
	// Add broker lock to check the topic exist
	b.mu.Lock()
	_, ok := b.topics[data.Topic]
	if !ok {
		b.newTopic(data.Topic)
	}
	b.mu.Unlock()

	// Use mutex lock to lock writing
	b.topics[data.Topic].mu.Lock()
	defer b.topics[data.Topic].mu.Unlock()

	b.topics[data.Topic].MQ = append(b.topics[data.Topic].MQ, data.Data)
	return len(b.topics[data.Topic].MQ)
}

func (bs *brokerServer) producerHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/producer" {
		http.Error(w, "404 not found.", http.StatusNotFound)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "405 Method Not Allowed.", http.StatusMethodNotAllowed)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "500 Internal Server Error: ioutil.ReadAll body error.", http.StatusInternalServerError)
		return
	}
	var data postData
	err = json.Unmarshal(body, &data)
	if err != nil {
		http.Error(w, "500 Internal Server Error: json Unmarshal error.", http.StatusInternalServerError)
		return
	}

	offset := bs.broker.writeTopic(data)
	fmt.Fprintf(w, "%d", offset)

}

func wait() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGPIPE)
	for {
		s := <-sc
		if s == syscall.SIGPIPE {
			fmt.Println("broker.main.wait: SIGPIPE occured")
			continue
		}
		break
	}
}

// StartBroker create a broker and start the server
func StartBroker(p string) {
	mu := &sync.RWMutex{}
	topics := make(map[string]*Topic)
	b := Broker{
		topics: topics,
		mu:     mu,
	}

	bs := &brokerServer{broker: &b, mux: http.NewServeMux()}
	bs.mux.HandleFunc("/consumer", bs.consumerHandler)
	bs.mux.HandleFunc("/producer", bs.producerHandler)
	hs := http.Server{Addr: p, Handler: bs}

	defer func() {
		fmt.Println("broker.main: Stop server...")
		if err := hs.Shutdown(context.Background()); err != nil {
			fmt.Printf("broker.main: %v", err)
		}
		fmt.Println("broker.main: Stop server...DONE")
	}()

	go func() {
		fmt.Println("broker.main: Start server")
		if err := hs.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("broker.main: Failed to serve: %v\n", err)
		}
	}()

	wait()
}
