package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Metric struct {
	Timestamp int64
	Name      string
	Count     int64
	Value     float64
	Cpu       float64
	Tags      string
}

var (
	in_addr   = flag.String("in", "", "incoming socket")
	dump_type = flag.String("type", "php", "incoming socket")

	server_name = flag.String("server", "", "server")
	hostname    = flag.String("hostname", "", "hostname")
	script_name = flag.String("script_name", "", "script_name")

	requests = flag.Bool("requests", true, "show requests")
	timers = flag.Bool("timers", false, "show timers")
)

// dumper --in=tcp://172.16.5.130:5003 --server=example.com

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(2)

	addr, err := net.ResolveTCPAddr("tcp4", *in_addr)
	if err != nil {
		log.Fatalf("[Dumper] ResolveTCPAddr: '%v'", err)
	}

	// TODO: implement reconnect
	server, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Fatalf("[Dumper] DialTCP: '%v'", err)
	}
	server.SetKeepAlive(true)
	log.Printf("[Dumper] Connected, start listening on tcp://%v\n", *in_addr)

	fmt.Printf("Waiting for data on %v\n", *in_addr)

	defer server.Close()
	dec := gob.NewDecoder(server)
	for {
		var data = make([]string, 0)
		err := dec.Decode(&data)
		if err != nil {
			log.Printf("[Dumper] Error on Decode: %v", err)
		}
		if len(data) == 0 {
			continue
		}

		start := time.Now()
		var buffer = make([]*Metric, len(data))
		for idx, m := range data {
			metric := strings.SplitAfterN(m, " ", 6)
			ts, err := strconv.ParseInt(strings.TrimSpace(metric[1]), 10, 32)
			if err != nil {
				log.Printf("[Dumper] Error on ParseInt: %v", err)
			}
			val, err := strconv.ParseFloat(strings.TrimSpace(metric[2]), 32)
			if err != nil {
				log.Printf("[Dumper] Error on ParseFloat: %v", err)
			}
			cnt, err := strconv.ParseInt(strings.TrimSpace(metric[3]), 10, 32)
			if err != nil {
				log.Printf("[Dumper] Error on ParseInt: %v", err)
			}
			cpu, err := strconv.ParseFloat(strings.TrimSpace(metric[4]), 32)
			if err != nil {
				log.Printf("[Dumper] Error on ParseFloat: %v", err)
			}

			buffer[idx] = &Metric{
				Name:      strings.TrimRight(strings.TrimSpace(metric[0]), " "),
				Timestamp: ts,
				Value:     val,
				Count:     cnt,
				Cpu:       cpu,
				Tags:      metric[5],
			}
		}

		log.Printf("[Dumper] Recive %d metrics in %v", len(buffer), time.Now().Sub(start))

		for _, request := range buffer {
			if hostname != nil && !strings.Contains(request.Tags, fmt.Sprintf("host=%s", *hostname)) {
				continue
			}
			if server_name != nil && !strings.Contains(request.Tags, fmt.Sprintf("server=%s", *server_name)) {
				continue
			}
			if script_name != nil && !strings.Contains(request.Tags, fmt.Sprintf("script=%s", *script_name)) {
				continue
			}

			if *requests && request.Name != "request" {
				continue
			}

			if *timers && request.Name != "timer" {
				continue
			}

			fmt.Printf("Request %s\n", request.Name)
			fmt.Printf("Timestamp %d\n", request.Timestamp)
			fmt.Printf("Tags %s\n", request.Tags)
			fmt.Printf("Value %3.4f\n", request.Value)
			fmt.Printf("Count %d\n", request.Count)
			fmt.Printf("Cpu %3.4f\n", request.Cpu)
			fmt.Printf("\n")
		}
	}
}
