package main

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	//	"net/http"
	"strings"
	"time"
)

type InfluxWriter struct {
	input chan []*Metric
	host  string
}

type InfluxMetric struct {
	Name      string            `json:"name"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}

func NewInfluxWriter(influxdb_addr *string, metrics chan []*Metric) (w *InfluxWriter) {
	w = &InfluxWriter{
		input: metrics,
		host:  *influxdb_addr,
	}
	return w
}

func (w *InfluxWriter) Start() {
	log.Printf("[InfluxWriter] Ready!")
	re := regexp.MustCompile("[^\\w\\d\\.\\/\\-\\_]")
	for {
		select {
		case metrics := <-w.input:
			log.Printf("[InfluxWriter] Get %v metrics for %v", len(metrics), metrics[0].Time)
			t := time.Now()
			var data = make([]*InfluxMetric, 0, len(metrics)*5)
			for _, m := range metrics {
				var tags = make(map[string]string)
				for _, tag := range strings.Split(m.Tags, " ") {
					tmp := strings.Split(tag, "=")
					if len(tmp) != 2 {
						continue
					}
					// may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'.
					tags[tmp[0]] = re.ReplaceAllString(tmp[1], "_")
				}
				ts := m.Time.Unix() * 1000

				data = append(data,
					&InfluxMetric{Name: fmt.Sprintf("%s.count", m.Name), Timestamp: ts, Value: float64(m.Count), Tags: tags},
					&InfluxMetric{Name: fmt.Sprintf("%s.max", m.Name), Timestamp: ts, Value: m.Max(), Tags: tags},
					&InfluxMetric{Name: fmt.Sprintf("%s.mean", m.Name), Timestamp: ts, Value: m.Mean(), Tags: tags},
					&InfluxMetric{Name: fmt.Sprintf("%s.p85", m.Name), Timestamp: ts, Value: m.Percentile(85), Tags: tags},
					&InfluxMetric{Name: fmt.Sprintf("%s.p95", m.Name), Timestamp: ts, Value: m.Percentile(95), Tags: tags},
				)
			}
			log.Printf("[InfluxWriter] Data ready in %v", time.Now().Sub(t))
			json_data, err := json.Marshal(data)
			if err != nil {
				log.Fatalf("[InfluxWriter] Failed to marshal data: %v", err)
			}
			log.Printf("[InfluxWriter] Json ready in %v", time.Now().Sub(t))
			log.Printf("json: %d", len(string(json_data)))
		}
	}
}
