package main

import (
	"flag"
	"log"
)

var (
	in_addr  = flag.String("in", "", "incoming socket")
	out_addr = flag.String("out", "", "outcoming socket")
)

func main() {
	flag.Parse()
	log.Printf("Pinba collector listening on %s and send to %s\n", *in_addr, *out_addr)

	stream := make(chan []byte, 10000)

	listener, err := NewListener(in_addr)
	if err != nil {
		log.Fatalf("[Listener] Can't resolve address: '%v'", err)
	}
	log.Printf("[Listener] Start listening on udp://%v\n", *in_addr)
	go listener.Start(stream)

	publisher := NewPublisher(out_addr)
	publisher.Start(stream)
}
