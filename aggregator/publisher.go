package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Writer struct {
	input chan []*RawMetric
	host  *net.TCPAddr
}

func NewWriter(addr *string, src chan []*RawMetric) (w *Writer) {
	host, err := net.ResolveTCPAddr("tcp4", *addr)
	if err != nil {
		log.Fatalf("ResolveTCPAddr: '%v'", err)
	}
	return &Writer{input: src, host: host}
}

func (w *Writer) Start() {
	log.Printf("Ready!")

	sock, err := net.DialTCP("tcp", nil, w.host)
	if err != nil {
		log.Printf("DialTCP: '%v'", err)
		return
	}
	defer sock.Close()

	metricsBuffer := NewMetrics(100000)
	prev := time.Now().Unix()
	cnt := 0

	for {
		select {
		case input := <-w.input:
			if len(input) == 0 {
				log.Printf("Input is empty\n")
				continue
			}

			t := time.Now()
			ts := input[0].Timestamp
			cnt += len(input)

			// If this is 10th second or it was more than 10 second since last flush
			if ts%10 == 0 || ts-prev > 10 {
				go w.send(sock, ts, metricsBuffer.Data, cnt)

				prev = ts
				cnt = 0
				metricsBuffer.Reset()
			}

			for _, m := range input {
				ts := m.Timestamp * 1000
				server, _ := m.Tags.Get("server")

				if m.Name == "request" {
					if server == "" || server == "unknown" {
						continue // no server tag :(
					}

					tags := m.Tags.Filter(&[]string{"status", "user", "type", "region"})
					metricsBuffer.Add(ts, tags, "php.requests", m.Count, m.Value, m.Cpu)

					tags = m.Tags.Filter(&[]string{"script", "status", "user", "type", "region"})
					metricsBuffer.Add(ts, tags, "php.requests."+server, m.Count, m.Value, m.Cpu)

				} else if m.Name == "timer" {
					if server == "" || server == "unknown" {
						continue // no server tag :(
					}

					group, err := m.Tags.Get("group")
					if err != nil {
						continue // no group tag :(
					}

					tags := m.Tags.Filter(&[]string{"server", "operation", "type", "region", "ns", "database"})
					metricsBuffer.Add(ts, tags, "php.timers."+group, m.Count, m.Value, 0)

					tags = m.Tags.Filter(&[]string{"script", "operation", "type", "region", "ns", "database"})
					metricsBuffer.Add(ts, tags, "php.timers."+server+"."+group, m.Count, m.Value, 0)

				} else {
					metricsBuffer.Add(ts, m.Tags.String(), m.Name, m.Count, m.Value, 0)
				}
			}
			log.Printf("Get %v metrics for %v, appended in %v",
				len(input), input[0].Timestamp, time.Now().Sub(t))
		}
	}
}

func (w *Writer) send(rw io.ReadWriter, ts int64, data map[string]*Metric, rawCount int) {
	var cnt int
	var buffer bytes.Buffer
	t := time.Now()
	timestamp := strconv.FormatInt(ts, 10)

	for _, m := range data {
		if strings.HasSuffix(m.Name, ".cpu") {
			cpu := m.Percentile(95)
			if cpu > 0 { // if cpu usage is zero, don't send it, it's not interesting
				buffer.WriteString(m.Put(timestamp, "", cpu))
				cnt += 1
			}
		} else {
			buffer.WriteString(m.Put(timestamp, ".rps", float64(m.Count)/10))
			buffer.WriteString(m.Put(timestamp, ".p10", m.Percentile(10)))
			buffer.WriteString(m.Put(timestamp, ".p85", m.Percentile(85)))
			buffer.WriteString(m.Put(timestamp, ".p95", m.Percentile(95)))
			buffer.WriteString(m.Put(timestamp, ".max", m.Max()))
			cnt += 5
		}

		if cnt%1000 == 0 {
			if _, err := rw.Write(buffer.Bytes()); err != nil {
				log.Fatalf("[Writer] Failed to write data: %v, line was: %v",
					err, buffer.String())
				continue
			}
			var response []byte
			n, err := rw.Read(response)
			if err != nil && err != io.EOF {
				log.Printf("[Writer] Failed to read from tsdb: %v", err)
			}
			if n > 0 {
				log.Printf("[Writer] TSDB says: %v", response)
			}
			buffer.Reset()
		}
	}
	if _, err := rw.Write(buffer.Bytes()); err != nil {
		log.Fatalf("[Writer] Failed to write data: %v, line was: %v",
			err, buffer.String())
	}
	var response []byte
	n, err := rw.Read(response)
	if err != nil && err != io.EOF {
		log.Printf("[Writer] Failed to read from tsdb: %v", err)
	}
	if n > 0 {
		log.Printf("[Writer] TSDB says: %v", response)
	}

	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)
	selfstat := fmt.Sprintf("put pinba.aggregator.count %v %d type=php\n", timestamp, cnt) +
		fmt.Sprintf("put pinba.aggregator.time %v %3.4f type=php\n", timestamp, time.Since(t).Seconds()) +
		fmt.Sprintf("put pinba.aggregator.metrics %v %d type=php\n", timestamp, rawCount) +
		fmt.Sprintf("put pinba.aggregator.goroutines %v %d type=php\n", timestamp, runtime.NumGoroutine()) +
		fmt.Sprintf("put pinba.aggregator.memory.allocated %v %d type=php\n", timestamp, memStats.Alloc) +
		fmt.Sprintf("put pinba.aggregator.memory.mallocs %v %d type=php\n", timestamp, memStats.Mallocs) +
		fmt.Sprintf("put pinba.aggregator.memory.frees %v %d type=php\n", timestamp, memStats.Frees) +
		fmt.Sprintf("put pinba.aggregator.memory.heap %v %d type=php\n", timestamp, memStats.HeapAlloc) +
		fmt.Sprintf("put pinba.aggregator.memory.stack %v %d type=php\n", timestamp, memStats.StackInuse)

	rw.Write([]byte(selfstat))

	log.Printf("[Writer] %v unique metrics sent to OpenTSDB in %v (%v)", cnt, time.Since(t), timestamp)
}