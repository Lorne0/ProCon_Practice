package main

import (
	"encoding/json"
	"fmt"
	"procon"
)

func main() {
	UserA := procon.NewConsumer()
	data, err := UserA.Consume(":7122", "weather", -1, -1)
	if err != nil {
		fmt.Printf("UserA.Consume error: %v\n", err)
		return
	}
	js, _ := json.MarshalIndent(data, "", "  ")
	fmt.Println(string(js))
}
