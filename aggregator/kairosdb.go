package main

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"net/http"
	"strings"
	"time"
)

type KairosWriter struct {
	input chan []*Metric
	host  string
}

type KairosMetric struct {
	Name      string            `json:"name"`
	Timestamp int64             `json:"timestamp"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
}

func NewKairosWriter(kairos_addr *string, metrics chan []*Metric) (w *KairosWriter) {
	w = &KairosWriter{
		input: metrics,
		host:  *kairos_addr,
	}
	return w
}

func (w *KairosWriter) Start() {
	log.Printf("[KairosWriter] Ready!")
	re := regexp.MustCompile("[^\\w\\d\\.\\/\\-\\_]")
	for {
		select {
		case metrics := <-w.input:
			if len(metrics) == 0 {
				continue
			}
			// 750k metrics every 10 seconds
			log.Printf("[KairosWriter] Get %v metrics for %v", len(metrics), metrics[0].Time)
			t := time.Now()
			var data = make([]*KairosMetric, 0, len(metrics))
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
					&KairosMetric{Name: m.Name, Timestamp: ts, Value: m.Value(), Tags: tags},

					// &KairosMetric{Name: fmt.Sprintf("%s.count", m.Name), Timestamp: ts, Value: float64(m.Count), Tags: tags},
					// &KairosMetric{Name: fmt.Sprintf("%s.max", m.Name), Timestamp: ts, Value: m.Max(), Tags: tags},
					// &KairosMetric{Name: fmt.Sprintf("%s.mean", m.Name), Timestamp: ts, Value: m.Mean(), Tags: tags},
					// &KairosMetric{Name: fmt.Sprintf("%s.p85", m.Name), Timestamp: ts, Value: m.Percentile(85), Tags: tags},
					// &KairosMetric{Name: fmt.Sprintf("%s.p95", m.Name), Timestamp: ts, Value: m.Percentile(95), Tags: tags},
				)
			}


			// Benchmark!



			// if len(data) == 10000 {
			// post_t := time.Now()
			// w.Post(data)
			// data = make([]*KairosMetric, 0, 1000)
			// log.Printf("[KairosWriter] POST in %v", time.Now().Sub(post_t))
			// }
			log.Printf("[KairosWriter] Data ready in %v (%d)", time.Now().Sub(t), len(data))
			// json_data, err := json.Marshal(data)
			// if err != nil {
			// 	log.Fatalf("[KairosWriter] Failed to marshal data: %v", err)
			// }
			// log.Printf("[KairosWriter] Json ready in %v", time.Now().Sub(t))
			// log.Printf("json: %d", len(string(json_data)))


			// 	defer response.Body.Close()
			// 	contents, err := ioutil.ReadAll(response.Body)
			// 	if err != nil {
			// 		fmt.Printf("%s", err)
			// 		os.Exit(1)
			// 	}
			// 	fmt.Printf("%s\n", string(contents))
			// }

		}
	}
}

func (w *KairosWriter) Post(data []*KairosMetric) {
	json_data, json_err := json.Marshal(data)
	if json_err != nil {
		log.Fatalf("[KairosWriter] Failed to marshal data: %v", json_err)
	}

	response, err := http.Post(fmt.Sprintf("http://%s/api/v1/datapoints", w.host), "application/json", strings.NewReader(string(json_data)))
	if err != nil {
		log.Printf("POST error: %s", err)
	}
	response.Body.Close()
}
