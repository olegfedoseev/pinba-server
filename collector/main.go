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
)

func main() {
	flag.Parse()

	log.Printf("Pinba collector listening on %s and send to %s\n", *in_addr, *out_addr)
	log.Printf("Using %d/%d CPU\n", *cpu, runtime.NumCPU())
	runtime.GOMAXPROCS(*cpu)

	listener := NewListener(in_addr)
	listener.Start()

	publisher := NewPublisher(out_addr, listener.Data)
	publisher.Start()
}
