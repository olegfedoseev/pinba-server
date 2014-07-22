package main

import (
	"log"
	"time"
)

type Aggregator struct {
	input   chan []*RawMetric
	Metrics chan []*Metric
}

func NewAggregator(raw chan []*RawMetric) (a *Aggregator) {
	a = &Aggregator{
		input:   raw,
		Metrics: make(chan []*Metric),
	}
	return a
}

func (a *Aggregator) Start(interval int64) {
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	tick := time.Now().Add(time.Second * time.Duration(interval))

	var buffer = make(map[string]*Metric)
	var t time.Duration

	for {
		select {
		case metrics := <-a.input:
			s := time.Now()
			for _, m := range metrics {
				if _, ok := buffer["cpu_"+m.Tags]; !ok {
					buffer["cpu_"+m.Tags] = NewMetric(tick, m.Name+".cpu", m.Tags)
				}
				buffer["cpu_"+m.Tags].Add(m.Count, m.Cpu)

				if _, ok := buffer["value_"+m.Tags]; !ok {
					buffer["value_"+m.Tags] = NewMetric(tick, m.Name, m.Tags)
				}
				buffer["value_"+m.Tags].Add(m.Count, m.Value)
			}
			t += time.Now().Sub(s)

		case tick = <-ticker.C:
			log.Printf("[Aggregator] Tick! Got %v raw metrics, Appended in %v", len(buffer), t)
			t = 0
			var result = make([]*Metric, 0, len(buffer))
			for _, val := range buffer {
				result = append(result, val)
			}
			a.Metrics <- result
			buffer = make(map[string]*Metric)
		}
	}
}
