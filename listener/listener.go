package listener

import (
	"encoding/gob"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

type Listener struct {
	RawMetrics chan []*RawMetric
	server     *net.TCPConn
}

type RawMetric struct {
	Timestamp int64
	Name      string
	Count     int64
	Value     float64
	Cpu       float64
	Tags      string
}

func NewListener(in_addr *string) (l *Listener) {
	addr, err := net.ResolveTCPAddr("tcp4", *in_addr)
	if err != nil {
		log.Fatalf("[Listener] ResolveTCPAddr: '%v'", err)
	}

	// TODO: implement reconnect
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Fatalf("[Listener] DialTCP: '%v'", err)
	}
	sock.SetKeepAlive(true)
	log.Printf("[Listener] Start listening on tcp://%v\n", *in_addr)

	l = &Listener{
		server:     sock,
		RawMetrics: make(chan []*RawMetric, 10000),
	}
	return l
}

func (l *Listener) Start() {
	defer l.server.Close()
	dec := gob.NewDecoder(l.server)
	for {
		var data = make([]string, 0)
		err := dec.Decode(&data)
		if err != nil {
			log.Printf("[Listener] Error on Decode: %v", err)
		}
		if len(data) == 0 {
			continue
		}

		start := time.Now()
		var buffer = make([]*RawMetric, len(data))
		for idx, m := range data {
			metric := strings.SplitAfterN(m, " ", 6)
			ts, err := strconv.ParseInt(strings.TrimSpace(metric[1]), 10, 32)
			if err != nil {
				log.Printf("[Listener] Error on ParseInt: %v", err)
			}
			val, err := strconv.ParseFloat(strings.TrimSpace(metric[2]), 32)
			if err != nil {
				log.Printf("[Listener] Error on ParseFloat: %v", err)
			}
			cnt, err := strconv.ParseInt(strings.TrimSpace(metric[3]), 10, 32)
			if err != nil {
				log.Printf("[Listener] Error on ParseInt: %v", err)
			}
			cpu, err := strconv.ParseFloat(strings.TrimSpace(metric[4]), 32)
			if err != nil {
				log.Printf("[Listener] Error on ParseFloat: %v", err)
			}

			buffer[idx] = &RawMetric{
				Name:      strings.TrimRight(strings.TrimSpace(metric[0]), " "),
				Timestamp: ts,
				Value:     val,
				Count:     cnt,
				Cpu:       cpu,
				Tags:      metric[5],
			}
		}

		log.Printf("[Listener] Recive %d metrics in %v", len(buffer), time.Now().Sub(start))
		l.RawMetrics <- buffer
	}
}
