package main

/*

!!!! https://github.com/rcrowley/go-metrics/blob/master/influxdb/influxdb.go

*/

import (
	"flag"
	"log"
	"runtime"
	"net"
	"encoding/gob"
)

var (
	in_addr  = flag.String("in", "", "incoming socket")
	out_addr = flag.String("out", "", "outcoming socket")
	cpu      = flag.Int("cpu", 1, "how much cores to use")
	gzip     = flag.Bool("gzip", false, "use gzip to compress outbound data")
)

func main() {
	flag.Parse()

	log.Printf("Pinba aggregator reading from %s and sending to %s\n", *in_addr, *out_addr)
	log.Printf("Using %d/%d CPU\n", *cpu, runtime.NumCPU())
	runtime.GOMAXPROCS(*cpu)

	addr, err := net.ResolveTCPAddr("tcp4", *in_addr)
	if err != nil {
		log.Fatalf("[Publisher] ResolveTCPAddr: '%v'", err)
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Fatalf("[Publisher] DialTCP: '%v'", err)
	}
	conn.SetKeepAlive(true)
	dec := gob.NewDecoder(conn)

	for {
		var data = make([]string, 0)
		err := dec.Decode(&data)
		if err != nil {
			log.Printf("err: %v", err)
		}
		if len(data) > 0 {
			log.Printf("len: %v", len(data))
		}
	}
}
