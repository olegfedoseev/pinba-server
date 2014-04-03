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

	log.Printf("Pinba server listening on %s and send to %s\n", *in_addr, *out_addr)
	log.Printf("Using %d/%d CPU\n", *cpu, runtime.NumCPU())
	runtime.GOMAXPROCS(*cpu)

	listener := NewListener(in_addr)
	listener.Start()

	decoder := NewDecoder(listener.RawPackets, *cpu)
	decoder.Start()

	publisher := NewPublisher(out_addr, decoder.Decoded)
	publisher.Start()
}
