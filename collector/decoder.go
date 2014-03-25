package main

import (
	"log"
	"strings"
	"time"
)

type Decoder struct {
	Raw     chan RawData
	Decoded chan string
	timers  chan time.Duration
}

type Worker struct {
	Data   chan RawData
	Result chan<- string
	Timer  chan<- time.Duration
}

func NewDecoder(raw chan RawData, workers int) *Decoder {
	decoder := &Decoder{
		Raw:     raw,
		Decoded: make(chan string, 100),
		timers:  make(chan time.Duration, 100),
	}
	for i := 0; i < workers; i++ {
		decoder.NewWorker()
	}
	return decoder
}

func (d *Decoder) NewWorker() {
	worker := &Worker{
		Data: d.Raw,
		Result: d.Decoded,
		Timer:  d.timers,
	}
	go func() {
		for {
			select {
			case data := <-worker.Data:
				start := time.Now()
				metrics, err := Decode(data.Timestamp.Unix(), data.Data)
				if err != nil {
					log.Printf("[Decoder] Error decoding protobuf packet: %v", err)
					//log.Printf("var data = %#v \n", data)
					return
				}

				worker.Result <- strings.Join(metrics, "")
				worker.Timer <- time.Now().Sub(start)
			}
		}
	}()
}

// queue chan RawData, ndecoders int
func (d *Decoder) Start() {
	go func() {
		var decoding_time time.Duration
		var decoded_count = 0
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				log.Printf("Packets: %d (in %v cpu time)", decoded_count, decoding_time)
				decoded_count = 0
				decoding_time = 0
			case t := <-d.timers:
				decoding_time += t
				decoded_count += 1
			}
		}
	}()
}
