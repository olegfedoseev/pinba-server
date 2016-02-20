package main

import (
	"fmt"
	"log"
	"net"
	"net/url"
	"runtime"
	"strings"
	"time"

	"bosun.org/collect"
	"bosun.org/opentsdb"
)

type Writer struct {
	input         chan []*RawMetric
	config        *Config
	metricsBuffer *Metrics
}

func NewWriter(config *Config, src chan []*RawMetric) (*Writer, error) {
	_, err := net.ResolveTCPAddr("tcp4", config.TSDBhost)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve %q: %v", config.TSDBhost, err)
	}

	w := &Writer{
		config:        config,
		input:         src,
		metricsBuffer: NewMetrics(100000),
	}
	return w, nil
}

func (w *Writer) Start() {

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

			for _, m := range input {
				// server tag is mandatory
				server, _ := m.Tags.Get("server")
				if server == "" || server == "unknown" {
					continue // no server tag :(
				}

				for _, metric := range w.config.Metrics {
					if m.Name != metric.Type {
						continue
					}

					// We can't have metrics without tags
					tags := m.Tags.Filter(metric.Tags)
					if len(tags) == 0 {
						continue
					}

					if len(metric.ReqiredTags) > 0 &&
						len(m.Tags.Filter(metric.ReqiredTags)) != len(metric.ReqiredTags) {
						continue
					}

					name := w.config.Prefix + m.Tags.Stringf(metric.Name)

					w.metricsBuffer.Add(tags, name, m.Count, m.Value)

					// If for this metric we also want CPU time, then add it
					// with different name
					if metric.CPUTime {
						w.metricsBuffer.Add(tags, name+".cpu", m.Count, m.Cpu)
					}
				}
			}

			// If this is 10th second or it was more than 10 second since last flush
			if ts%10 == 0 || ts-prev > 10 {
				go w.send(ts, w.metricsBuffer.Data, cnt)

				prev = ts
				cnt = 0
				w.metricsBuffer.Reset()
			}

			log.Printf("Get %v metrics for %v, appended in %v",
				len(input), input[0].Timestamp, time.Now().Sub(t))
		}
	}
}

func (w *Writer) send(ts int64, data map[string]*Metric, rawCount int) error {
	var dps opentsdb.MultiDataPoint
	batchSize := 1000
	t := time.Now()
	putUrl := (&url.URL{Scheme: "http", Host: w.config.TSDBhost, Path: "api/put"}).String()

	for _, m := range data {
		if strings.HasSuffix(m.Name, ".cpu") {
			cpu := m.Percentile(95)
			if cpu > 0 { // if cpu usage is zero, don't send it, it's not interesting
				dps = append(dps, &opentsdb.DataPoint{m.Name, ts, cpu, m.Tags})
			}
		} else {
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".rps", ts, float64(m.Count) / 10, m.Tags})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".p25", ts, m.Percentile(25), m.Tags})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".p50", ts, m.Percentile(50), m.Tags})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".p75", ts, m.Percentile(75), m.Tags})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".p95", ts, m.Percentile(95), m.Tags})
			dps = append(dps, &opentsdb.DataPoint{m.Name + ".max", ts, m.Max(), m.Tags})
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

	typeTag := opentsdb.TagSet{"type": w.config.Prefix}

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
