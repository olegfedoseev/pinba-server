package main

import (
	"flag"
	"log"
)

func main() {
	var (
		inAddr  = flag.String("in", "", "incoming socket")
		outAddr = flag.String("out", "", "outcoming socket")
	)
	flag.Parse()
	log.Printf("Pinba collector listening on %s and send to %s\n", *inAddr, *outAddr)

	stream := make(chan []byte, 10000)

	listener, err := NewListener(inAddr)
	if err != nil {
		log.Fatalf("Can't resolve address: '%v'", err)
	}
	log.Printf("Start listening on udp://%v\n", *inAddr)
	go listener.Start(stream)

	publisher, err := NewPublisher(outAddr)
	if err != nil {
		log.Fatalf("Can't resolve address: '%v'", err)
	}
	log.Printf("Start listening on tcp://%v\n", *outAddr)
	publisher.Start(stream)
}
