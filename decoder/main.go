package main

import (
	"flag"
	zmq "github.com/pebbe/zmq4"
	"log"
	"strings"
	"time"
)

var (
	in_addr  = flag.String("in", "", "incoming socket")
	out_addr = flag.String("out", "", "outcoming socket")
)

func worker(source <-chan RawData, decoded chan<- string, timers chan<- time.Duration) {
	for {
		select {
		case pb := <-source:
			start := time.Now()
			metrics, err := Decode(pb.Timestamp, pb.Data)
			if err != nil {
				log.Printf("[Decoder] Error decoding protobuf packet: %v", err)
				continue
			}
			for _, m := range metrics {
				decoded <- m
			}
			timers <- time.Since(start)
		}
	}
}

func main() {
	flag.Parse()
	log.Printf("Pinba decoder listening on %s and sending to %s\n", *in_addr, *out_addr)

	listener := NewListener(in_addr)

	publisher, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		log.Fatalf("[Decoder] Failed to create socket: %v", err)
	}
	defer publisher.Close()
	if err := publisher.Bind(*out_addr); err != nil {
		log.Fatalf("[Decoder] Failed to bind to %v: %v", *out_addr, err)
	}

	var decoding_time time.Duration
	var decoded_count, sent_count int64
	decoded := make(chan string, 10000)
	timers := make(chan time.Duration, 1000)
	ticker := time.NewTicker(time.Second)

	// Let's go!
	for i := 0; i < *cpu; i++ {
		go worker(listener.RawMetrics, decoded, timers)
	}
	go listener.Start()

	for {
		select {
		case <-ticker.C:
			log.Printf("[Decoder] Decoded %v (%v), sent %v", decoded_count, decoding_time, sent_count)
			sent_count = 0
			decoded_count = 0
			decoding_time = 0

		case metric := <-decoded:
			sent_count += 1
			// request 1421892675 0.000000 1 0.000000 host=frontend2 server=www.kem-rabota.ru script=/favicon.ico status=200
			data := strings.SplitAfterN(metric, " ", 2)
			if _, err := publisher.Send(strings.TrimSpace(data[0]), zmq.SNDMORE); err != nil {
				log.Printf("[Decoder] Failed to SendMessage: %v", err)
			}
			if _, err := publisher.Send(data[1], 0); err != nil {
				log.Printf("[Decoder] Failed to SendMessage: %v", err)
			}

		case t := <-timers:
			decoding_time += t
			decoded_count += 1
		}
	}
}
