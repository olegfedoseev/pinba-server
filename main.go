package main

import (
	//	"bytes"
	//	"os"
	//	"compress/zlib"
	"flag"
	"fmt"
	"log"
	"net"
	//"os"
	"runtime"
	"time"
)

// func sender(out_addr *string, packets *[][]byte) {
// 	c := time.Tick(1 * time.Second)
// 	for now := range c {
// 		log.Printf("%v, %v, %v \n", now.Unix(), len(buffer.Bytes()), counter)
// 		if counter == 0 {
// 			continue
// 		}

// 		for counter > 0 {
// 			log.Printf("%v, %d\n", counter, len(*packets))
// 			counter--
// 		}

// 		// 	var b bytes.Buffer
// 		// 	w := zlib.NewWriter(&b)
// 		// 	//w.Write(int32(counter))
// 		// 	w.Write(buffer.Bytes())
// 		// 	w.Close()
// 		// 	//sock.WriteToUDP([]byte(fmt.Sprintf("%d\n%s", now.Unix(), b.Bytes())), c.userAddr)
// 		// 	//publisher.Send(fmt.Sprintf("%d\n%s", now.Unix(), b.Bytes()), 0)
// 		// }
// 		// buffer = *bytes.NewBuffer([]byte{})
// 		counter = 0
// 	}
// }

func reciver(result chan<- *Metric) {
	addr, err := net.ResolveUDPAddr("udp4", *in_addr)
	if err != nil {
		fmt.Printf("Error on net.ResolveUDPAddr, %v", err)
		panic(err)
	}
	sock, err := net.ListenUDP("udp4", addr)
	if err != nil {
		fmt.Printf("Error on net.ListenUDP, %v", err)
		panic(err)
	}
	log.Printf("Start listening on %v\n", *in_addr)

	for {
		var buf = make([]byte, 65536)
		rlen, _, err := sock.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Error on sock.ReadFrom, %v", err)
			panic(err)
		}
		if rlen == 0 {
			continue
		}

		packets_cnt++
		go func(data []byte, result chan<- *Metric) {
			metrics, err := Decode(time.Now().Unix(), data)
			if err != nil {
				log.Printf("Error on decode %v", err)
			} else {
				for _, m := range metrics {
					result <- m
				}
			}
		}(buf[0:rlen], result)
	}
}

var (
	packets_cnt = 0
	in_addr     = flag.String("in", "", "incoming socket")
	out_addr    = flag.String("out", "", "outcoming socket")
	cpu         = flag.Int("cpu", 0, "how much cores to use")
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")

func main() {
	flag.Parse()
	fmt.Printf("Pinba server listening on %s and send to %s\n", *in_addr, *out_addr)

	if *cpu == 0 {
		fmt.Printf("Using all avalible cores\n")
		runtime.GOMAXPROCS(runtime.NumCPU())
	} else {
		fmt.Printf("Using %d cores\n", *cpu)
		runtime.GOMAXPROCS(*cpu)
	}

	//input := make(chan [][]byte)

	// go func () {
	// 	addr, err := net.ResolveUDPAddr("udp4", *out_addr)
	// 	if err != nil {
	// 		fmt.Printf("Error on net.ResolveUDPAddr, %v", err)
	// 		panic(err)
	// 	}
	// 	sock, err := net.ListenUDP("udp4", addr)
	// 	if err != nil {
	// 		fmt.Printf("Error on net.ListenUDP, %v", err)
	// 		panic(err)
	// 	}
	// 	clients []*net.UDPAddr
	// 	for {
	// 		rlen, clientAddr, err := sock.ReadFromUDP(buf[0:])
	// 		if err != nil {
	// 			log.Printf("Error on sock.ReadFrom, %v", err)
	// 			panic(err)
	// 		}
	// 		log.Printf("New client from %v", clientAddr)
	// 		clients.appe
	// 	}
	// }

	var in_buffer = make([]*Metric, 0)
	var packets = make(chan *Metric)
	var ticks = make(chan time.Time)

	go reciver(packets)
	go func(ticks chan<- time.Time) {
		for now := range time.Tick(time.Second) {
			ticks <- now
		}
	}(ticks)

	//go sender(out_addr, &packets)

	for {
		select {
		case now := <-ticks:
			log.Printf("Tick! %v, %d/%v (%v)\n", now.Unix(), packets_cnt, len(in_buffer), DecodeTime)
			if DecodeTime > time.Second {
				log.Printf("Processing took more than 1 second (%v)!\n", DecodeTime)
			}
			DecodeTime = 0
			packets_cnt = 0
			in_buffer = make([]*Metric, 0)
		case data := <-packets:
			in_buffer = append(in_buffer, data)
		}
	}
}
