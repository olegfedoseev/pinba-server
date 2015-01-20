package main

import (
	"flag"
	"github.com/olegfedoseev/pinba-server/listener"
	"log"
	"runtime"
)

var (
	in_addr  = flag.String("in", "", "incoming socket")
	out_addr = flag.String("out", "", "out address")
	cpu      = flag.Int("cpu", 1, "how much cores to use")
)

func main() {
	flag.Parse()

	log.Printf("Pinba aggregator reading from %s\n", *in_addr)
	log.Printf("Using %d/%d CPU\n", *cpu, runtime.NumCPU())
	runtime.GOMAXPROCS(*cpu)

	listener := listener.NewListener(in_addr)
	go listener.Start()

	writer := NewWriter(out_addr, listener.RawMetrics)
	writer.Start()
}
