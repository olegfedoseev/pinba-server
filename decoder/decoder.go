package main

import (
	"fmt"
	"log"
	"time"
)

type Decoder struct {
	Raw     chan RawData
	Decoded chan []string
	timers  chan time.Duration
}

func NewDecoder(raw chan RawData, workers int) *Decoder {
	decoder := &Decoder{
		Raw:     raw, // RawPackets from listener
		Decoded: make(chan []string, 1000),
		timers:  make(chan time.Duration, 1000),
	}
	for i := 0; i < workers; i++ {
		go decoder.worker()
	}
	return decoder
}

func (d *Decoder) worker() {
	for {
		select {
		case data := <-d.Raw:
			start := time.Now()
			metrics, err := Decode(data.Timestamp, data.Data)
			if err != nil {
				log.Printf("[Decoder] Error decoding protobuf packet: %v", err)
				continue
			}

			d.Decoded <- metrics
			d.timers <- time.Since(start)
		}
	}
}

func (d *Decoder) Start() {
	var decoding_time time.Duration
	var decoded_count = 0
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			log.Printf("[Decoder] Packets: %d (in %v cpu time)", decoded_count, decoding_time)
			// Append stats for self monitoring
			d.Decoded <- []string{
				fmt.Sprintf("pinba.collector.decoder.time %d %3.4f 0 0.0", time.Now().Unix(), decoding_time.Seconds()),
				fmt.Sprintf("pinba.collector.decoder.decoded %d %d 0 0.0", time.Now().Unix(), decoded_count),
				fmt.Sprintf("pinba.collector.decoder.in_queue %d %d 0 0.0", time.Now().Unix(), len(d.Raw)),
				fmt.Sprintf("pinba.collector.decoder.out_queue %d %d 0 0.0", time.Now().Unix(), len(d.Decoded)),
			}
			decoded_count = 0
			decoding_time = 0
		case t := <-d.timers:
			decoding_time += t
			decoded_count += 1
		}
	}
}
