package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/olegfedoseev/pinba-server/client"
)

func main() {
	var (
		inAddr     = flag.String("in", "", "incoming socket")
		configFile = flag.String("config", "config.yml", "config name, default - config.yml")
	)
	flag.Parse()

	config, err := NewConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config from %v: %v", *configFile, err)
	}

	pinba, err := client.New(*inAddr, 5*time.Second, 5*time.Second)
	if err != nil {
		log.Fatalf("Failed to create pinba client: %v", err)
	}
	go pinba.Listen(config.Interval)

	metrics := make(chan []*RawMetric, 10)
	writer, err := NewWriter(config, metrics)
	if err != nil {
		log.Fatalf("Failed to create OpenTSDB writer: %v", err)
	}
	go writer.Start()

	fmt.Printf("Reading from %q\n", *inAddr)
	fmt.Printf("OpenTSDB at %q\n", config.TSDBhost)
	fmt.Printf("Interval is %d\n", config.Interval)
	fmt.Printf("Prefix is %q\n", config.Prefix)
	fmt.Println()

	buffer := make([]*RawMetric, 0)

	for {
		select {
		case requests := <-pinba.Requests:
			t := time.Now()
			for _, request := range requests.Requests {
				buffer = append(buffer, &RawMetric{
					Timestamp: requests.Timestamp,
					Name:      "request",
					Count:     1,
					Value:     request.RequestTime,
					Cpu:       request.RuUtime + request.RuStime,
					Tags:      request.Tags,
				})

				for _, timer := range request.Timers {
					buffer = append(buffer, &RawMetric{
						Timestamp: requests.Timestamp,
						Name:      "timer",
						Count:     int64(timer.HitCount),
						Value:     timer.Value,
						Cpu:       timer.RuUtime + timer.RuStime,
						Tags:      timer.Tags,
					})
				}
			}
			log.Printf("Convert %v requests to %v RawMetrics for %v in %v",
				len(requests.Requests), len(buffer), requests.Timestamp, time.Since(t))

			metrics <- buffer
			buffer = make([]*RawMetric, 0)
		}
	}
}
