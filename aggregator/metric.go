package main

import (
	//	"log"
	"math"
	"sort"
	"time"
)

type Metric struct {
	Time   time.Time
	Name   string
	Count  int64
	values []float64
	Tags   string
	length int64
}

func sum(values []float64) (sum float64) {
	for _, value := range values {
		sum += value
	}
	return
}

func NewMetric(ts time.Time, name string, tags string) (m *Metric) {
	return &Metric{Time: ts, Name: name, Tags: tags, Count: 0, length: 0}
}

func (m *Metric) Add(cnt int64, val float64) {
	m.Count += cnt
	m.values = append(m.values, val)
	m.length += 1
}

func (m *Metric) Max() float64 {
	sort.Float64s(m.values)
	return m.values[m.length-1]
}

func (m *Metric) Mean() float64 {
	sort.Float64s(m.values)
	return m.values[m.length/2]
}

func (m *Metric) Stdev() float64 {
	if m.length == 0 {
		return 0.0
	}

	sort.Float64s(m.values)

	mean := sum(m.values) / float64(m.length)
	var variance float64
	for _, val := range m.values {
		variance += (val - mean) * (val - mean)
	}

	return math.Sqrt(variance / float64(m.length-1))
}

func (m *Metric) Percentile(rank int) float64 {
	if m.length == 0 {
		return 0.0
	}

	sort.Float64s(m.values)
	pos := float64(rank) / 100 * float64(m.length)
	ipos := int64(pos)

	if ipos < 1 {
		return m.values[0]
	} else if ipos >= m.length {
		return m.values[m.length-1]
	}
	lower := m.values[ipos-1]
	upper := m.values[ipos]
	return (pos-math.Floor(pos))*(upper-lower) + lower
}

func (m *Metric) Value() float64 {
	if len(m.values) == 0 {
		return 0.0
	}
	return m.values[0]
}
