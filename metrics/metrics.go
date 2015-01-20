package metrics

import (
	"math"
	"sort"
	"strconv"
	"bytes"
)

type Metrics struct {
	size int64
	Count int64
	Data map[string]*Metric
}

func NewMetrics(size int64) (m *Metrics) {
	return &Metrics{Count: 0, Data: make(map[string]*Metric, size), size: size}
}

func (m *Metrics) Add(ts int64, tags, name string, count int64, value, cpu float64) {
	id := name + tags
	if _, ok := m.Data[id]; !ok {
		m.Data[id] = NewMetric(ts, name, tags)
	}
	m.Data[id].Add(count, value)

	id = name + ".cpu" + tags
	if _, ok := m.Data[id]; !ok {
		m.Data[id] = NewMetric(ts, name + ".cpu", tags)
	}
	m.Data[id].Add(0, cpu)
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
	Tags   string
	length int64
}

func sum(values []float64) (sum float64) {
	for _, value := range values {
		sum += value
	}
	return
}

func NewMetric(ts int64, name string, tags string) (m *Metric) {
	return &Metric{Time: strconv.FormatInt(ts, 10), Name: name, Tags: tags, Count: 0, length: 0}
}

func (m *Metric) Add(cnt int64, val float64) {
	m.Count += cnt
	m.Values = append(m.Values, val)
	m.length += 1
}

func (m *Metric) Max() float64 {
	sort.Float64s(m.Values)
	return m.Values[m.length-1]
}

func (m *Metric) Mean() float64 {
	sort.Float64s(m.Values)
	return m.Values[m.length/2]
}

func (m *Metric) Stdev() float64 {
	if m.length == 0 {
		return 0.0
	}

	sort.Float64s(m.Values)

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

	sort.Float64s(m.Values)
	pos := float64(rank) / 100 * float64(m.length)
	ipos := int64(pos)

	if ipos < 1 {
		return m.Values[0]
	} else if ipos >= m.length {
		return m.Values[m.length-1]
	}
	lower := m.Values[ipos-1]
	upper := m.Values[ipos]
	return (pos-math.Floor(pos))*(upper-lower) + lower
}

func (m *Metric) Value() float64 {
	if len(m.Values) == 0 {
		return 0.0
	}
	return m.Values[0]
}

func (m* Metric) Put(name string, value float64) string {
	var buffer bytes.Buffer
	buffer.WriteString("put ")
	buffer.WriteString(m.Name)
	buffer.WriteString(name)
	buffer.WriteString(" ")
	buffer.WriteString(m.Time)
	buffer.WriteString(" ")
	buffer.WriteString(strconv.FormatFloat(value, 'f', 4, 64))
	buffer.WriteString(" ")
	buffer.WriteString(m.Tags)
	buffer.WriteString("\n")
	return buffer.String()
}
