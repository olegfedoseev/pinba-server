package main

import (
	"flag"
	zmq "github.com/pebbe/zmq4"
	"log"
	"runtime"
	"time"
)

var (
	in_addr  = flag.String("in", "", "incoming socket")
	out_addr = flag.String("out", "", "out address")
	cpu      = flag.Int("cpu", 1, "how much cores to use")
)

func main() {
	flag.Parse()

	log.Printf("Pinba aggregator reading from %s\n", *in_addr)
	log.Printf("Using %d/%d CPU\n", *cpu, runtime.NumCPU())
	runtime.GOMAXPROCS(*cpu)

	subscriber, _ := zmq.NewSocket(zmq.SUB)
	defer subscriber.Close()
	subscriber.Connect(*in_addr)
	subscriber.SetSubscribe("request")
	subscriber.SetSubscribe("timer")

	var metrics = make(chan []*RawMetric, 10000)
	var buffer = make([]*RawMetric, 0)
	var metric *RawMetric
	ts := int64(time.Now().Unix())
	log.Printf("Starting with ts %v", ts)

	writer := NewWriter(out_addr, metrics)
	go writer.Start()

	for {
		msg, err := subscriber.RecvMessage(0)
		if err != nil {
			log.Printf("Failed to recive message: %v", err)
		}
		if metric, err = NewRawMetric(msg[0], msg[1]); err != nil {
			break
		}
		if metric.Timestamp > ts {
			metrics <- buffer
			buffer = make([]*RawMetric, 0)
			ts = metric.Timestamp
		}

		buffer = append(buffer, metric)
	}
}
