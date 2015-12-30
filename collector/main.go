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

	listener := NewListener(in_addr)
	listener.Start()

	publisher := NewPublisher(out_addr, listener.Data)
	publisher.Start()
}
