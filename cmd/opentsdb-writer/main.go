package main

import (
	"flag"
	"log"
	"time"

	"github.com/olegfedoseev/pinba-server/client"
)

func main() {
	var (
		inAddr   = flag.String("in", "", "incoming socket")
		outAddr  = flag.String("out", "", "out address")
		prefix   = flag.String("prefix", "php", "prefix for metrics names, default - php")
		interval = flag.Int64("interval", 10, "interval for aggregation, default - 10 (sec)")
	)
	flag.Parse()

	log.Printf("Pinba aggregator reading from %s\n", *inAddr)

	pinba, err := client.New(*inAddr, 5*time.Second, 5*time.Second)
	if err != nil {
		log.Fatalln(err)
	}
	go pinba.Listen(*interval)

	metrics := make(chan []*RawMetric, 10)
	writer := NewWriter(*prefix, *outAddr, metrics)
	go writer.Start()

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
