package main

import "github.com/Lorne0/procon"

func main() {
	WeatherBureau := procon.NewProducer()
	data := map[string]interface{}{"weather": "sunny", "temperature": "23"}
	WeatherBureau.Produce(":7122", "weather", data)
}
