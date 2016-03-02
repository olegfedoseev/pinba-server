package main

import (
	"math"
	"sort"

	"github.com/olegfedoseev/opentsdb"
	"github.com/olegfedoseev/pinba"
)

type MetricsSettings struct {
	Name        string   `yaml:"name"`
	Tags        []string `yaml:"tags"`
	Type        string   `yaml:"type"`
	ReqiredTags []string `yaml:"required"`
	CPUTime     bool     `yaml:"cpu"`
}

type Metrics struct {
	size  int
	Count int64
	Data  map[string]*Metric
}

func NewMetrics(size int) (m *Metrics) {
	return &Metrics{Count: 0, Data: make(map[string]*Metric, size), size: size}
}

func (m *Metrics) Add(tags pinba.Tags, name string, count int64, value float32) {
	id := name + tags.String()
	if _, ok := m.Data[id]; !ok {
		m.Data[id] = NewMetric(name, tags)
	}
	m.Data[id].Add(count, float64(value))
	m.Count += 1
}

func (m *Metrics) Reset() {
	m.Count = 0
	m.Data = make(map[string]*Metric, m.size)
}

type Metric struct {
	Name   string
	Count  int64
	Values []float64
	Tags   opentsdb.Tags
	sorted bool
}

func sum(values []float64) (sum float64) {
	for _, value := range values {
		sum += value
	}
	return
}

func NewMetric(name string, tags pinba.Tags) (m *Metric) {
	tagsMap := make(opentsdb.Tags)
	for _, tag := range tags {
		tagsMap.Set(tag.Key, tag.Value)
	}

	return &Metric{
		Name:  name,
		Tags:  tagsMap,
		Count: 0,
	}
}

func (m *Metric) Add(cnt int64, val float64) {
	m.Count += cnt
	m.Values = append(m.Values, val)
	m.sorted = false
}

func (m *Metric) IsEmpty() bool {
	return len(m.Values) == 0
}

func (m *Metric) Max() float64 {
	m.sort()
	return m.Values[len(m.Values)-1]
}

func (m *Metric) Median() float64 {
	m.sort()
	return m.Values[len(m.Values)/2]
}

func (m *Metric) Stdev() float64 {
	avg := sum(m.Values) / float64(len(m.Values))

	var variance float64
	for _, val := range m.Values {
		variance += (val - avg) * (val - avg)
	}

	return math.Sqrt(variance / float64(len(m.Values)))
}

func (m *Metric) Percentile(rank int) float64 {
	m.sort()
	percent := float64(rank) / 100
	k := float64(len(m.Values)-1) * percent
	f := math.Floor(k)
	c := math.Ceil(k)

	if f == c {
		return m.Values[int(k)]
	}
	d0 := m.Values[int(f)] * (c - k)
	d1 := m.Values[int(c)] * (k - f)
	return d0 + d1
}

func (m *Metric) Value() float64 {
	return m.Values[0]
}

func (m *Metric) sort() {
	if !m.sorted {
		sort.Float64s(m.Values)
		m.sorted = true
	}
}
