package main

import (
	zmq "github.com/pebbe/zmq4"
	"log"
)

func receive(addr string, filter []string) <-chan []string {
	subscriber, _ := zmq.NewSocket(zmq.SUB)
	subscriber.Connect(addr)

	for _, pattern := range filter {
		subscriber.SetSubscribe(pattern)
	}

	out := make(chan []string)

	go func() {
		defer subscriber.Close()
		for {
			msg, err := subscriber.RecvMessage(0)
			if err != nil {
				log.Printf("Failed to recive message: %v", err)
				break
			}
			out <- msg
		}
		close(out)
	}()

	return out
}
