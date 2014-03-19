package main

import (
	"flag"
	"log"
	"runtime"
)

var (
	in_addr  = flag.String("in", "", "incoming socket")
	out_addr = flag.String("out", "", "outcoming socket")
	cpu      = flag.Int("cpu", 1, "how much cores to use")
	gzip     = flag.Bool("gzip", false, "use gzip to compress outbound data")
)

func main() {
	flag.Parse()

	log.Printf("Pinba server listening on %s and send to %s\n", *in_addr, *out_addr)
	log.Printf("Using %d/%d CPU\n", *cpu, runtime.NumCPU())
	runtime.GOMAXPROCS(*cpu)

	var messages = make(chan []string, 10)
	listener := NewListener(in_addr, messages)
	go listener.Run()

	publisher := NewPublisher(out_addr, messages, *gzip)
	publisher.Run()
}
