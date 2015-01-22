package main

import (
	"bytes"
	"log"
	"net"
	"strings"
	"time"
)

type Writer struct {
	input chan []*RawMetric
	host  string
}

func NewWriter(addr *string, src chan []*RawMetric) (w *Writer) {
	return &Writer{input: src, host: *addr}
}

func (w *Writer) Start() {
	log.Printf("[Writer] Ready!")

	addr, err := net.ResolveTCPAddr("tcp4", w.host)
	if err != nil {
		log.Fatalf("[Writer] ResolveTCPAddr: '%v'", err)
	}

	// TODO: implement reconnect ?
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Fatalf("[Writer] DialTCP: '%v'", err)
	}
	defer sock.Close()
	sock.SetKeepAlive(true)
	log.Printf("[Writer] Connected to tcp://%v\n", w.host)

	ticker := time.NewTicker(10 * time.Second)

	metricsBuffer := NewMetrics(100000)

	for {
		select {
		case <-ticker.C:

			var cnt int
			var buffer bytes.Buffer
			t := time.Now()
			log.Printf("Tick! %v %v", len(metricsBuffer.Data), metricsBuffer.Count)
			for _, m := range metricsBuffer.Data {
				if strings.HasSuffix(m.Name, ".cpu") {
					cpu := m.Percentile(95)
					if cpu > 0 { // if cpu usage is zero, don't send it, it's not interesting
						buffer.WriteString(m.Put("", cpu))
						cnt += 1
					}
				} else {
					buffer.WriteString(m.Put(".rps", float64(m.Count)/10))
					buffer.WriteString(m.Put(".p85", m.Percentile(85)))
					buffer.WriteString(m.Put(".p95", m.Percentile(95)))
					buffer.WriteString(m.Put(".max", m.Max()))
					cnt += 4
				}

				if cnt%1000 == 0 {
					if _, err = sock.Write(buffer.Bytes()); err != nil {
						log.Fatalf("[Writer] Failed to write data: %v, line was: %v",
							err, buffer.String())
						continue
					}
					buffer.Reset()
				}
			}
			if _, err = sock.Write(buffer.Bytes()); err != nil {
				log.Fatalf("[Writer] Failed to write data: %v, line was: %v",
					err, buffer.String())
				continue
			}

			log.Printf("[Writer] %v metrics writen in %v", cnt, time.Since(t))
			metricsBuffer.Reset()

		case input := <-w.input:
			if len(input) == 0 {
				log.Printf("[Writer] Input is empty\n")
				continue
			}

			t := time.Now()
			for _, m := range input {
				ts := m.Timestamp * 1000

				if m.Name == "request" {
					server, err := m.Tags.Get("server")
					if err != nil {
						log.Printf("No server tag: %v %v", m.Name, m.Tags)
						continue // no server tag :(
					}

					tags := m.Tags.Filter(&[]string{"status", "user", "type", "region"})
					metricsBuffer.Add(ts, tags, "php.requests", m.Count, m.Value, m.Cpu)

					tags = m.Tags.Filter(&[]string{"script", "status", "user", "type", "region"})
					metricsBuffer.Add(ts, tags, "php.requests."+server, m.Count, m.Value, m.Cpu)
				} else if m.Name == "timer" {
					group, err := m.Tags.Get("group")
					if err != nil {
						//log.Printf("No group tag: %v", m.Tags)
						continue // no group tag :(
					}
					server, err := m.Tags.Get("server")
					if err != nil {
						continue // no server tag :(
					}

					tags := m.Tags.Filter(&[]string{"server", "operation", "type", "region", "ns", "database"})
					metricsBuffer.Add(ts, tags, "php.timers."+group, m.Count, m.Value, 0)

					tags = m.Tags.Filter(&[]string{"script", "operation", "type", "region", "ns", "database"})
					metricsBuffer.Add(ts, tags, "php.timers."+server+"."+group, m.Count, m.Value, 0)

				} else {
					metricsBuffer.Add(ts, m.Tags.String(), m.Name, m.Count, m.Value, 0)
				}
			}
			log.Printf("[Writer] Get %v metrics for %v, appended in %v",
				len(input), input[0].Timestamp, time.Now().Sub(t))
		}
	}
}
