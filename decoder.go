package main

import (
	"log"
	"strings"
	"time"
)

type Decoder struct {
	Raw     chan RawData
	Decoded chan string
	workers chan chan RawData
	timers  chan time.Duration
}

type Worker struct {
	Data   chan RawData
	Pool   chan chan RawData
	Result chan<- string
	Timer  chan<- time.Duration
}

func NewDecoder(raw chan RawData, workers int) *Decoder {
	decoder := &Decoder{
		Raw:     raw,
		Decoded: make(chan string, 100),
		workers: make(chan chan RawData, workers),
		timers:  make(chan time.Duration, 100),
	}
	for i := 0; i < workers; i++ {
		decoder.NewWorker()
	}
	return decoder
}

func (d *Decoder) NewWorker() {
	worker := &Worker{
		Data:   make(chan RawData),
		Pool:   d.workers,
		Result: d.Decoded,
		Timer:  d.timers,
	}
	go func() {
		for {
			// Add ourselves into the worker queue.
			worker.Pool <- worker.Data

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
		var decoder_time time.Duration
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case data := <-d.Raw:
				go func() {
					worker_queue := <-d.workers
					worker_queue <- data
				}()
			case now := <-ticker.C:
				log.Printf("%v Time: %v", now.Format("15:04:05"), decoder_time)
				decoder_time = 0
			case t := <-d.timers:
				decoder_time += t
			}
		}
	}()
}
