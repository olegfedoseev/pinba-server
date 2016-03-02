package main

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/olegfedoseev/opentsdb"
	"github.com/olegfedoseev/pinba-server/client"
)

type Writer struct {
	config        *writerConfig
	metricsBuffer *Metrics
	prefix        string
	client        *opentsdb.Client

	timersSettings   []MetricsSettings
	requestsSettings []MetricsSettings
}

func NewWriter(config *writerConfig) (*Writer, error) {
	_, err := net.ResolveTCPAddr("tcp4", config.TSDB.Host)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve %q: %v", config.TSDB.Host, err)
	}

	client, err := opentsdb.NewClient(
		config.TSDB.Host,
		config.BufferSize,
		time.Duration(config.TSDB.Timeout*1000)*time.Microsecond,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenTSDB client: %v", err)
	}
	client.StartWorkers(config.Workers, config.BatchSize, 100*time.Millisecond)

	w := &Writer{
		prefix:           config.Prefix,
		metricsBuffer:    NewMetrics(config.BufferSize),
		client:           client,
		timersSettings:   make([]MetricsSettings, 0),
		requestsSettings: make([]MetricsSettings, 0),
	}

	for _, metric := range config.Metrics {
		if metric.Type == "request" {
			w.requestsSettings = append(w.requestsSettings, metric)
		}
		if metric.Type == "timer" {
			w.timersSettings = append(w.timersSettings, metric)
		}
	}

	return w, nil
}

func (w *Writer) Start(requestsChan chan *client.PinbaRequests) {
	statsTag := opentsdb.Tags{"type": w.prefix}

	for {
		select {
		case timer := <-w.client.Clock:
			log.Printf("[INFO][%d] POSTed to OpenTSDB in %v", timer.Timestamp, timer.Stop.Sub(timer.Start))
			w.client.Push(&opentsdb.DataPoint{
				"pinba.aggregator.time",
				timer.Timestamp,
				timer.Stop.Sub(timer.Start),
				statsTag,
			})

		case err := <-w.client.Errors:
			log.Printf("[ERROR] OpenTSDB Client error: %v", err)

		case requests := <-requestsChan:
			t := time.Now()
			for _, request := range requests.Requests {
				// server tag is mandatory
				server, _ := request.Tags.Get("server")
				if server == "" || server == "unknown" {
					continue // no server tag :(
				}

				for _, config := range w.requestsSettings {
					// We can't have metrics without tags
					tags := request.Tags.Filter(config.Tags)
					if len(tags) == 0 {
						continue
					}

					if len(config.ReqiredTags) > 0 &&
						len(request.Tags.Filter(config.ReqiredTags)) != len(config.ReqiredTags) {
						continue
					}

					name := w.prefix + request.Tags.Stringf(config.Name)

					w.metricsBuffer.Add(tags, name, 1, request.RequestTime)

					// If for this metric we also want CPU time, then add it
					// with different name
					if config.CPUTime {
						w.metricsBuffer.Add(tags, name+".cpu", 1, request.RuUtime+request.RuStime)
					}
				}

				for _, config := range w.timersSettings {
					for _, timer := range request.Timers {
						// We can't have metrics without tags
						tags := timer.Tags.Filter(config.Tags)
						if len(tags) == 0 {
							continue
						}

						if len(config.ReqiredTags) > 0 &&
							len(timer.Tags.Filter(config.ReqiredTags)) != len(config.ReqiredTags) {
							continue
						}

						name := w.prefix + timer.Tags.Stringf(config.Name)

						w.metricsBuffer.Add(tags, name, int64(timer.HitCount), timer.Value)

						// If for this metric we also want CPU time, then add it
						// with different name
						if config.CPUTime {
							w.metricsBuffer.Add(tags, name+".cpu", int64(timer.HitCount), timer.RuUtime+timer.RuStime)
						}
					}
				}
			}

			log.Printf("[DEBUG] Queue: %v, Sent: %v, Dropped: %v", len(w.client.Queue), w.client.Sent, w.client.Dropped)

			go w.send(requests.Timestamp, w.metricsBuffer.Data)
			w.metricsBuffer.Reset()

			d := time.Since(t)
			log.Printf("[INFO][%d] Get %v metrics, appended in %v",
				requests.Timestamp, len(requests.Requests), d-d%time.Millisecond)

			w.client.Push(&opentsdb.DataPoint{
				"pinba.aggregator.metrics",
				requests.Timestamp,
				len(requests.Requests),
				statsTag,
			})
		}
	}
}

func (w *Writer) send(ts int64, data map[string]*Metric) {
	t := time.Now()

	var total int
	for _, m := range data {
		if strings.HasSuffix(m.Name, ".cpu") {
			cpu := m.Percentile(95)
			if cpu > 0 { // if cpu usage is zero, don't send it, it's not interesting
				total++
				w.client.Push(&opentsdb.DataPoint{m.Name, ts, cpu, m.Tags})
			}
		} else {
			w.client.Push(&opentsdb.DataPoint{m.Name + ".rps", ts, float64(m.Count) / 10, m.Tags})
			w.client.Push(&opentsdb.DataPoint{m.Name + ".p25", ts, m.Percentile(25), m.Tags})
			w.client.Push(&opentsdb.DataPoint{m.Name + ".p50", ts, m.Percentile(50), m.Tags})
			w.client.Push(&opentsdb.DataPoint{m.Name + ".p75", ts, m.Percentile(75), m.Tags})
			w.client.Push(&opentsdb.DataPoint{m.Name + ".p95", ts, m.Percentile(95), m.Tags})
			w.client.Push(&opentsdb.DataPoint{m.Name + ".max", ts, m.Max(), m.Tags})
			total += 6
		}
	}

	d := time.Since(t)
	log.Printf("[INFO][%d] %v unique metrics sent to OpenTSDB in %v", ts, total, d-d%time.Millisecond)
}
