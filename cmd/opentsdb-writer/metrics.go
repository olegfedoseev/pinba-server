package main

import (
	"math"
	"regexp"
	"sort"
	"strconv"

	"bosun.org/opentsdb"
	"github.com/olegfedoseev/pinba"
)

type RawMetric struct {
	Timestamp int64
	Name      string
	Count     int64
	Value     float32
	Cpu       float32
	Tags      pinba.Tags
}

func TagSet(t *pinba.Tags) opentsdb.TagSet {
	sort.Sort(t)
	ts := make(opentsdb.TagSet)
	// may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'.
	re := regexp.MustCompile("[^\\w\\d\\.\\/\\-\\_]")

	for _, tag := range *t {
		ts[tag.Key] = re.ReplaceAllString(tag.Value, "_")
	}
	return ts
}

type Metrics struct {
	size  int64
	Count int64
	Data  map[string]*Metric
}

func NewMetrics(size int64) (m *Metrics) {
	return &Metrics{Count: 0, Data: make(map[string]*Metric, size), size: size}
}

func (m *Metrics) Add(ts int64, tags pinba.Tags, name string, count int64, value, cpu float32) {
	id := name + tags.String()
	if _, ok := m.Data[id]; !ok {
		m.Data[id] = NewMetric(ts, name, tags)
	}
	m.Data[id].Add(count, float64(value))

	id = name + ".cpu" + tags.String()
	if _, ok := m.Data[id]; !ok {
		m.Data[id] = NewMetric(ts, name+".cpu", tags)
	}
	m.Data[id].Add(0, float64(cpu))
	m.Count += 1
}

func (m *Metrics) Reset() {
	m.Count = 0
	m.Data = make(map[string]*Metric, m.size)
}

type Metric struct {
	Time   string
	Name   string
	Count  int64
	Values []float64
	Tags   opentsdb.TagSet
	length int64
	sorted bool
}

func sum(values []float64) (sum float64) {
	for _, value := range values {
		sum += value
	}
	return
}

func NewMetric(ts int64, name string, tags pinba.Tags) (m *Metric) {
	return &Metric{
		Time:   strconv.FormatInt(ts, 10),
		Name:   name,
		Tags:   TagSet(&tags),
		Count:  0,
		length: 0,
	}
}

func (m *Metric) Add(cnt int64, val float64) {
	m.Count += cnt
	m.Values = append(m.Values, val)
	m.length += 1
	m.sorted = false
}

func (m *Metric) Max() float64 {
	m.sort()
	return m.Values[m.length-1]
}

func (m *Metric) Median() float64 {
	m.sort()
	return m.Values[m.length/2]
}

func (m *Metric) Stdev() float64 {
	if m.length == 0 {
		return 0.0
	}

	m.sort()

	mean := sum(m.Values) / float64(m.length)
	var variance float64
	for _, val := range m.Values {
		variance += (val - mean) * (val - mean)
	}

	return math.Sqrt(variance / float64(m.length-1))
}

func (m *Metric) Percentile(rank int) float64 {
	if m.length == 0 {
		return 0.0
	}

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
	if len(m.Values) == 0 {
		return 0.0
	}
	return m.Values[0]
}

func (m *Metric) sort() {
	if !m.sorted {
		sort.Float64s(m.Values)
		m.sorted = true
	}
}
