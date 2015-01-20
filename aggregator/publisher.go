package main

import (
	"github.com/olegfedoseev/pinba-server/listener"
	"github.com/olegfedoseev/pinba-server/metrics"
	//	"fmt"
	"bytes"
	"log"
	"net"
	//	"strconv"
	"strings"
	"time"
)

type Writer struct {
	input chan []*listener.RawMetric
	host  string
}

func NewWriter(addr *string, src chan []*listener.RawMetric) (w *Writer) {
	return &Writer{input: src, host: *addr}
}

func (w *Writer) Start() {
	log.Printf("[Writer] Ready!")

	addr, err := net.ResolveTCPAddr("tcp4", w.host)
	if err != nil {
		log.Fatalf("[Writer] ResolveTCPAddr: '%v'", err)
	}

	// TODO: implement reconnect
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Fatalf("[Writer] DialTCP: '%v'", err)
	}
	defer sock.Close()
	sock.SetKeepAlive(true)
	log.Printf("[Writer] Connected to tcp://%v\n", w.host)

	ticker := time.NewTicker(10 * time.Second)

	metricsBuffer := metrics.NewMetrics(20000)

	for {
		select {
		case <-ticker.C:

			var cnt int
			var buffer bytes.Buffer
			var d1, d2 time.Duration
			t := time.Now()
			log.Printf("Tick! %v %v", len(metricsBuffer.Data), metricsBuffer.Count)
			for _, m := range metricsBuffer.Data {
				// put <metric> <timestamp> <value> <tagk1=tagv1[ tagk2=tagv2 ...tagkN=tagvN]>
				// [rps|cpu|p85|p95|max]
				t1 := time.Now()
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
				d1 += time.Since(t1)

				if cnt%1000 == 0 {
					t2 := time.Now()
					if _, err = sock.Write(buffer.Bytes()); err != nil {
						log.Fatalf("[Writer] Failed to write data: %v, line was: %v",
							err, buffer.String())
						continue
					}
					buffer.Reset()
					d2 += time.Since(t2)
				}
			}

			t2 := time.Now()
			if _, err = sock.Write(buffer.Bytes()); err != nil {
				log.Fatalf("[Writer] Failed to write data: %v, line was: %v",
					err, buffer.String())
				continue
			}
			d2 += time.Since(t2)

			log.Printf("[Writer] Data writen in %v (%d), %v, %v", time.Now().Sub(t), cnt, d1, d2)
			metricsBuffer.Reset()

		case input := <-w.input:
			if len(input) == 0 {
				log.Printf("[Writer] Input is empty\n")
				continue
			}

			t := time.Now()
			for _, m := range input {
				if m.Name != "request" {
					continue
				}
				ts := m.Timestamp * 1000

				//php.requests.[rps|cpu|p85|p95|max] [val] status=200 user=guest is_ajax=no region=66
				if m.Name == "request" {
					tags := m.Tags.Filter(&map[string]bool{"status": true, "user": true, "type": true, "region": true})
					metricsBuffer.Add(ts, tags, "php.requests", m.Count, m.Value, m.Cpu)

					tags = m.Tags.Filter(&map[string]bool{"script": true, "status": true, "user": true, "type": true, "region": true})
					server, err := m.Tags.Get("server")
					if err != nil {
						continue // no server tag :(
					}
					metricsBuffer.Add(ts, tags, "php.requests."+server, m.Count, m.Value, m.Cpu)
				}

				if m.Name == "timers" {
					group, err := m.Tags.Get("group")
					if err != nil {
						continue // no group tag :(
					}
					tags := m.Tags.Filter(&map[string]bool{"server": true, "operation": true, "type": true, "region": true, "ns": true, "database": true})
					metricsBuffer.Add(ts, tags, "php.timers"+group, m.Count, m.Value, m.Cpu)
				}
			}
			log.Printf("[Writer] Get %v metrics for %v, appended in %v",
				len(input), input[0].Timestamp, time.Now().Sub(t))
		}
	}
}
