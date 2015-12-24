package main

import (
	"bytes"
	"errors"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Tag struct {
	Key   string
	Value string
}

type Tags []Tag

func (t Tags) Len() int           { return len(t) }
func (t Tags) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t Tags) Less(i, j int) bool { return t[i].Key < t[j].Key }

func (t Tags) String() string {
	sort.Sort(t)
	var b bytes.Buffer
	for i, tag := range t {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(tag.Key)
		b.WriteString("=")
		b.WriteString(tag.Value)
	}
	return b.String()
}

func (t Tags) Get(key string) (string, error) {
	for _, tag := range t {
		if tag.Key == key {
			return tag.Value, nil
		}
	}
	return "", errors.New("no such tag")
}

var tags_buffer map[string]string

func (t Tags) Filter(filter *[]string) Tags {
	sort.Sort(t)
	var b Tags

	if tags_buffer == nil {
		tags_buffer = make(map[string]string, 0)
	}

	// may only contain alphanumeric characters plus periods '.', slash '/', dash '-', and underscore '_'.
	re := regexp.MustCompile("[^\\w\\d\\.\\/\\-\\_]")
	for _, tag := range t {
		if filter != nil {
			skip := true
			for _, f := range *filter {
				if f == tag.Key {
					skip = false
					break
				}
			}
			if skip {
				continue
			}
		}
		if tag.Value == "" {
			continue
		}

		if _, ok := tags_buffer[tag.Value]; !ok {
			tags_buffer[tag.Value] = re.ReplaceAllString(tag.Value, "_")
		}

		b = append(b, Tag{tag.Key, tags_buffer[tag.Value]})
	}
	return b
}

type RawMetric struct {
	Timestamp int64
	Name      string
	Count     int64
	Value     float64
	Cpu       float64
	Tags      Tags
}

func NewRawMetric(name, line string) (*RawMetric, error) {
	metric := strings.SplitAfterN(line, " ", 5)
	if len(metric) < 4 {
		return nil, errors.New("Field count mismatch")
	}

	ts, err := strconv.ParseInt(strings.TrimSpace(metric[0]), 10, 32)
	if err != nil {
		return nil, err
	}
	val, err := strconv.ParseFloat(strings.TrimSpace(metric[1]), 32)
	if err != nil {
		return nil, err
	}
	cnt, err := strconv.ParseInt(strings.TrimSpace(metric[2]), 10, 32)
	if err != nil {
		return nil, err
	}
	cpu, err := strconv.ParseFloat(strings.TrimSpace(metric[3]), 32)
	if err != nil {
		return nil, err
	}

	var tags Tags
	if len(metric) >= 5 {
		tmp := strings.Split(metric[4], " ")
		for _, tag := range tmp {
			kv := strings.Split(tag, "=")
			if len(kv) < 2 {
				continue
			}
			if kv[1] == "" {
				continue
			}
			tags = append(tags, Tag{kv[0], kv[1]})
		}
	}

	return &RawMetric{
		Name:      name,
		Timestamp: ts,
		Value:     val,
		Count:     cnt,
		Cpu:       cpu,
		Tags:      tags,
	}, nil
}

type Metrics struct {
	size  int64
	Count int64
	Data  map[string]*Metric
}

func NewMetrics(size int64) (m *Metrics) {
	return &Metrics{Count: 0, Data: make(map[string]*Metric, size), size: size}
}

func (m *Metrics) Add(ts int64, tags Tags, name string, count int64, value, cpu float64) {
	id := name + tags.String()
	if _, ok := m.Data[id]; !ok {
		m.Data[id] = NewMetric(ts, name, tags)
	}
	m.Data[id].Add(count, value)

	id = name + ".cpu" + tags.String()
	if _, ok := m.Data[id]; !ok {
		m.Data[id] = NewMetric(ts, name+".cpu", tags)
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
	Tags   Tags
	length int64
	sorted bool
}

func sum(values []float64) (sum float64) {
	for _, value := range values {
		sum += value
	}
	return
}

func NewMetric(ts int64, name string, tags Tags) (m *Metric) {
	return &Metric{Time: strconv.FormatInt(ts, 10), Name: name, Tags: tags, Count: 0, length: 0}
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

// put <metric> <timestamp> <value> <tagk1=tagv1[ tagk2=tagv2 ...tagkN=tagvN]>
func (m *Metric) Put(ts, name string, value float64) string {
	var buffer bytes.Buffer
	buffer.WriteString("put ")
	buffer.WriteString(m.Name)
	buffer.WriteString(name)
	buffer.WriteString(" ")
	buffer.WriteString(ts)
	buffer.WriteString(" ")
	buffer.WriteString(strconv.FormatFloat(value, 'f', 4, 64))
	buffer.WriteString(" ")
	buffer.WriteString(m.Tags.String())
	buffer.WriteString("\n")
	return buffer.String()
}

func (m *Metric) sort() {
	if !m.sorted {
		sort.Float64s(m.Values)
		m.sorted = true
	}
}
