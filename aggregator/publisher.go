package main

import (
	"fmt"
	"log"
	"net"
	"runtime"
	"strings"
	"time"
	"net/url"

	"bosun.org/opentsdb"
	"bosun.org/collect"
)

type Writer struct {
	input chan []*RawMetric
	addr *string
}

func NewWriter(addr *string, src chan []*RawMetric) (w *Writer) {
	_, err := net.ResolveTCPAddr("tcp4", *addr)
	if err != nil {
		log.Fatalf("ResolveTCPAddr: '%v'", err)
	}
	return &Writer{input: src, addr: addr}
}

func (w *Writer) Start() {
	log.Printf("Ready!")

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
				go w.send(ts, metricsBuffer.Data, cnt)

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

					tags := m.Tags.Filter(&[]string{"server", "user", "category", "type", "region"})
					metricsBuffer.Add(ts, tags, "php.requests", m.Count, m.Value, m.Cpu)

					tags = m.Tags.Filter(&[]string{"script", "status", "user", "category", "type", "region"})
					metricsBuffer.Add(ts, tags, "php.requests."+server, m.Count, m.Value, m.Cpu)

				} else if m.Name == "timer" {
					if server == "" || server == "unknown" {
						continue // no server tag :(
					}

					group, err := m.Tags.Get("group")
					if err != nil {
						continue // no group tag :(
					}

					tags := m.Tags.Filter(&[]string{"server", "operation", "category", "type", "region", "ns", "database"})
					metricsBuffer.Add(ts, tags, "php.timers."+group, m.Count, m.Value, 0)

					tags = m.Tags.Filter(&[]string{"script", "operation", "category", "type", "region", "ns", "database"})
					metricsBuffer.Add(ts, tags, "php.timers."+server+"."+group, m.Count, m.Value, 0)

				} else {
					metricsBuffer.Add(ts, m.Tags, m.Name, m.Count, m.Value, 0)
				}
			}
			log.Printf("Get %v metrics for %v, appended in %v",
				len(input), input[0].Timestamp, time.Now().Sub(t))
		}
	}
}

func (w *Writer) send(ts int64, data map[string]*Metric, rawCount int) (error) {
	var dps opentsdb.MultiDataPoint
	batchSize := 1000
	t := time.Now()
	putUrl := (&url.URL{Scheme: "http", Host: *w.addr, Path: "api/put"}).String()

	for _, m := range data {
		if strings.HasSuffix(m.Name, ".cpu") {
			cpu := m.Percentile(95)
			if cpu > 0 { // if cpu usage is zero, don't send it, it's not interesting
				dps = append(dps, &opentsdb.DataPoint{m.Name, ts, cpu, m.Tags.TagSet()})
			}
		} else {
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".rps", ts, float64(m.Count)/10, m.Tags.TagSet()})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".p25", ts, m.Percentile(25), m.Tags.TagSet()})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".p50", ts, m.Percentile(50), m.Tags.TagSet()})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".p75", ts, m.Percentile(75), m.Tags.TagSet()})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".p95", ts, m.Percentile(95), m.Tags.TagSet()})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".max", ts, m.Max(), m.Tags.TagSet()})
		}
	}

	total := 0
	for len(dps) > 0 {
		count := len(dps)
		if len(dps) > batchSize {
			count = batchSize
		}
		putResp, err := collect.SendDataPoints(dps[:count], putUrl)
		if err != nil {
			return err
		}
		defer putResp.Body.Close()

		if putResp.StatusCode != 204 {
			return fmt.Errorf("Non 204 status code from opentsdb: %d", putResp.StatusCode)
		}
		dps = dps[count:]
		total += count
	}

	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)

	typeTag := opentsdb.TagSet{"type": "php"}

	putResp, err := collect.SendDataPoints(opentsdb.MultiDataPoint{
		&opentsdb.DataPoint{"pinba.aggregator.count", ts, total, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.time", ts, time.Since(t).Seconds(), typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.metrics", ts, rawCount, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.goroutines", ts, runtime.NumGoroutine(), typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.allocated", ts, memStats.Alloc, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.mallocs", ts, memStats.Mallocs, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.frees", ts, memStats.Frees, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.heap", ts, memStats.HeapAlloc, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.stack", ts, memStats.StackInuse, typeTag},
	}, putUrl)
	if err != nil {
		return err
	}
	defer putResp.Body.Close()

	log.Printf("[Writer] %v unique metrics sent to OpenTSDB in %v (%v)", total, time.Since(t), ts)
	return nil
}
