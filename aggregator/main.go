package main

import (
	"flag"
	"log"
	"runtime"
	"strings"
	"time"
)

var (
	inAddr  = flag.String("in", "", "incoming socket")
	outAddr = flag.String("out", "", "out address")
	cpu     = flag.Int("cpu", 1, "how much cores to use")
	filter  = flag.String("filter", "request,timer", "filter metrics, accept request and timer, default - both")
	prefix  = flag.String("prefix", "php", "prefix for metrics names, default - php")
)

func main() {
	flag.Parse()

	log.Printf("Pinba aggregator reading from %s\n", *inAddr)
	log.Printf("Using %d/%d CPU\n", *cpu, runtime.NumCPU())
	runtime.GOMAXPROCS(*cpu)

	var metrics = make(chan []*RawMetric, 60) // 60 seconds buffer
	writer := NewWriter(*prefix, outAddr, metrics)
	go writer.Start()

	ts := int64(time.Now().Unix())
	buffer := make([]*RawMetric, 0)
	subscribeTo := strings.Split(*filter, ",")
	log.Printf("Subscribe to %v", subscribeTo)
	for msg := range receive(*inAddr, subscribeTo) {
		metric, err := NewRawMetric(msg[0], msg[1])
		if err != nil {
			log.Fatalf("Failed to get raw metrics: %v", err)
		}
		if metric.Timestamp > ts {
			ts = metric.Timestamp

			metrics <- buffer
			buffer = make([]*RawMetric, 0)
		}
		buffer = append(buffer, metric)
	}
}
