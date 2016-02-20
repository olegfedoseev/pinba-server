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
	"github.com/olegfedoseev/pinba-server/client"
)

type Writer struct {
	input         chan []*RawMetric
	config        *Config
	metricsBuffer *Metrics
	tsdbURL       string
}

func NewWriter(config *Config, src chan []*RawMetric) (*Writer, error) {
	_, err := net.ResolveTCPAddr("tcp4", config.TSDBhost)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve %q: %v", config.TSDBhost, err)
	}

	tsdbUrl := &url.URL{
		Scheme: "http",
		Host:   config.TSDBhost,
		Path:   "api/put",
	}

	w := &Writer{
		config:        config,
		input:         src,
		metricsBuffer: NewMetrics(100000),
		tsdbURL:       tsdbUrl.String(),
	}
	return w, nil
}

func (w *Writer) Start(requestsChan chan *client.PinbaRequests) {
	prev := time.Now().Unix()
	cnt := 0

	buffer := make([]*RawMetric, 0)
	for {
		select {
		case requests := <-requestsChan:
			t := time.Now()
			for _, request := range requests.Requests {
				// server tag is mandatory
				server, _ := request.Tags.Get("server")
				if server == "" || server == "unknown" {
					continue // no server tag :(
				}

				buffer = append(buffer, &RawMetric{
					Timestamp: requests.Timestamp,
					Name:      "request",
					Count:     1,
					Value:     request.RequestTime,
					Cpu:       request.RuUtime + request.RuStime,
					Tags:      request.Tags,
				})

				for _, timer := range request.Timers {
					buffer = append(buffer, &RawMetric{
						Timestamp: requests.Timestamp,
						Name:      "timer",
						Count:     int64(timer.HitCount),
						Value:     timer.Value,
						Cpu:       timer.RuUtime + timer.RuStime,
						Tags:      timer.Tags,
					})
				}
			}

			// Convert 95735 requests to 516653 RawMetrics for 1455687090 in 82ms
			d := time.Since(t)
			log.Printf("[INFO][%v] Convert %v requests to %v RawMetrics in %v",
				requests.Timestamp, len(requests.Requests), len(buffer), d-d%time.Millisecond)

			w.input <- buffer
			buffer = make([]*RawMetric, 0)

		case input := <-w.input:
			if len(input) == 0 {
				log.Printf("Input is empty\n")
				continue
			}

			t := time.Now()
			ts := input[0].Timestamp
			cnt += len(input)

			for _, m := range input {
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

			d := time.Since(t)
			log.Printf("[INFO][%d] Get %v metrics, appended in %v",
				input[0].Timestamp, len(input), d-d%time.Millisecond)
		}
	}
}

func (w *Writer) send(ts int64, data map[string]*Metric, rawCount int) error {
	t := time.Now()

	var dps opentsdb.MultiDataPoint
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
	batchSize := 100
	for len(dps) > 0 {
		count := len(dps)
		if count > batchSize {
			count = batchSize
		}
		putResp, err := collect.SendDataPoints(dps[:count], w.tsdbURL)
		if err != nil {
			return err
		}
		putResp.Body.Close()

		if putResp.StatusCode != 204 {
			return fmt.Errorf("non 204 status code from OpenTSDB: %d", putResp.StatusCode)
		}
		dps = dps[count:]
		total += count
	}

	if err := w.sendSelfStats(ts, total, rawCount, time.Since(t)); err != nil {
		return fmt.Errorf("failed to send self-stats: %v", err)
	}

	// [INFO][1455942780] 191072 unique metrics sent to OpenTSDB in 12.514s
	d := time.Since(t)
	log.Printf("[INFO][%d] %v unique metrics sent to OpenTSDB in %v", ts, total, d-d%time.Millisecond)
	return nil
}

func (w *Writer) sendSelfStats(ts int64, metrics, raw int, sendIn time.Duration) error {
	memStats := &runtime.MemStats{}
	runtime.ReadMemStats(memStats)

	typeTag := opentsdb.TagSet{"type": w.config.Prefix}

	response, err := collect.SendDataPoints(opentsdb.MultiDataPoint{
		&opentsdb.DataPoint{"pinba.aggregator.time", ts, sendIn.Seconds(), typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.count", ts, metrics, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.metrics", ts, raw, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.goroutines", ts, runtime.NumGoroutine(), typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.allocated", ts, memStats.Alloc, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.mallocs", ts, memStats.Mallocs, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.frees", ts, memStats.Frees, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.heap", ts, memStats.HeapAlloc, typeTag},
		&opentsdb.DataPoint{"pinba.aggregator.memory.stack", ts, memStats.StackInuse, typeTag},
	}, w.tsdbURL)
	if err != nil {
		return err
	}
	response.Body.Close()
	return nil
}
