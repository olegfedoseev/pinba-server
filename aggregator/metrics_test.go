package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	//	"time"
)

func TestNewRawMetrics(t *testing.T) {

	timer := "1421901818 0.0006 1 0.0000 host=phpnode9 server=example.com script=Script_Name status=200 region=123 type="
	request := "1421901938 0.013257 123 0.010000 host=phpnode8 server=example.com script status=404 region=234 type=public user=guest"

	m, err := NewRawMetric("timer", timer)
	assert.Nil(t, err)
	assert.Equal(t, "timer", m.Name)
	assert.EqualValues(t, 1421901818, m.Timestamp)
	//assert.EqualValues(t, 0.0006, m.Value)
	assert.EqualValues(t, 1, m.Count)
	assert.EqualValues(t, 5, len(m.Tags))
	// assert.EqualValues(t, 0.0, m.Cpu)

	m, err = NewRawMetric("request", request)
	assert.Nil(t, err)
	assert.Equal(t, "request", m.Name)
	assert.EqualValues(t, 1421901938, m.Timestamp)
	assert.EqualValues(t, 123, m.Count)
	assert.EqualValues(t, 6, len(m.Tags))
	// assert.EqualValues(t, 0.01, m.Cpu)

	m, err = NewRawMetric("request", "1421901938 0.013257 123 0.01")
	assert.Nil(t, err)
}

func TestNewRawMetricsWithErrors(t *testing.T) {
	_, err := NewRawMetric("timer", "123")
	assert.NotNil(t, err)

	_, err = NewRawMetric("timer", "a b c d e")
	assert.NotNil(t, err)
	assert.Equal(t, "strconv.ParseInt: parsing \"a\": invalid syntax", err.Error())

	_, err = NewRawMetric("timer", "123 b c d e")
	assert.NotNil(t, err)
	assert.Equal(t, "strconv.ParseFloat: parsing \"b\": invalid syntax", err.Error())

	_, err = NewRawMetric("timer", "123 0.0 c d e")
	assert.NotNil(t, err)
	assert.Equal(t, "strconv.ParseInt: parsing \"c\": invalid syntax", err.Error())

	_, err = NewRawMetric("timer", "123 0.0 0.0 d e")
	assert.NotNil(t, err)
	assert.Equal(t, "strconv.ParseInt: parsing \"0.0\": invalid syntax", err.Error())

	_, err = NewRawMetric("timer", "123 0.0 1 d e")
	assert.NotNil(t, err)
	assert.Equal(t, "strconv.ParseFloat: parsing \"d\": invalid syntax", err.Error())
}

func TestTagsToString(t *testing.T) {
	tags := Tags{Tag{"aaa", "val1"}, Tag{"bbb", "val2"}, Tag{"ccc", "val3"}}
	assert.Equal(t, "aaa=val1 bbb=val2 ccc=val3", tags.String())

	tags = Tags{Tag{"bbb", "val2"}, Tag{"aaa", "val1"}, Tag{"ccc", "val3"}}
	assert.Equal(t, "aaa=val1 bbb=val2 ccc=val3", tags.String())
}

func TestTagsGetTag(t *testing.T) {
	tags := Tags{Tag{"aaa", "val1"}, Tag{"bbb", "val2"}, Tag{"ccc", "val3"}}
	val, err := tags.Get("aaa")
	assert.Nil(t, err)
	assert.Equal(t, "val1", val)

	_, err = tags.Get("xxx")
	assert.NotNil(t, err)
}

func TestTagsFilter(t *testing.T) {
	tags := Tags{Tag{"aaa", "val:1"}, Tag{"bbb", "valы2"}, Tag{"ccc", "val3"}}
	assert.Equal(t, "aaa=val_1 bbb=val_2", tags.Filter(&[]string{"aaa", "bbb"}).String())

	tags = Tags{Tag{"bbb", "val2"}, Tag{"aaa", "val1"}, Tag{"ccc", "val3"}}
	assert.Equal(t, "aaa=val1 bbb=val2", tags.Filter(&[]string{"aaa", "bbb"}).String())

	tags = Tags{Tag{"bbb", "val2"}, Tag{"aaa", ""}, Tag{"ccc", "val3"}}
	assert.Equal(t, "bbb=val2", tags.Filter(&[]string{"aaa", "bbb"}).String())
}

func TestSum(t *testing.T) {
	assert.EqualValues(t, 0, sum([]float64{}))
	assert.EqualValues(t, 0, sum([]float64{0}))
	assert.EqualValues(t, 1, sum([]float64{0, 1}))
	assert.EqualValues(t, 6, sum([]float64{1, 2, 3}))
	assert.EqualValues(t, 7, sum([]float64{1.5, 2.5, 3}))
}

func TestNewMetric(t *testing.T) {
	metric := NewMetric(123, "test.metric", Tags{Tag{"aaa", "val_1"}, Tag{"bbb", "val_2"}})
	assert.Equal(t, "123", metric.Time)
	assert.Equal(t, "test.metric", metric.Name)
	assert.EqualValues(t, 0, metric.Count)
	assert.EqualValues(t, 0, metric.Value())

	assert.EqualValues(t, 0, metric.Percentile(0))
	assert.EqualValues(t, 0, metric.Percentile(25))
	assert.EqualValues(t, 0, metric.Percentile(75))
}

func TestMetricAdd(t *testing.T) {
	metric := NewMetric(123, "test.metric", Tags{Tag{"aaa", "val_1"}, Tag{"bbb", "val_2"}})
	assert.EqualValues(t, 0, metric.Count)

	metric.Add(1, 0.1)
	metric.Add(2, 0.2)
	metric.Add(3, 0.3)

	assert.EqualValues(t, 6, metric.Count)
	assert.EqualValues(t, 0.3, metric.Max())
	assert.EqualValues(t, 0.1, metric.Value())
}

func TestMetricPut(t *testing.T) {
	metric := NewMetric(123, "test.metric", Tags{Tag{"aaa", "val_1"}, Tag{"bbb", "val_2"}})

	assert.Equal(t, "put test.metric.p95 123 123.4560 aaa=val_1 bbb=val_2\n",
		metric.Put("123", ".p95", 123.456))
}

func TestMetricPercentile(t *testing.T) {
	metric := NewMetric(123, "test.metric", Tags{Tag{"aaa", "val_1"}, Tag{"bbb", "val_2"}})

	// 1.3,2.2,2.7,3.1,3.3,3.7

	metric.Add(1, 3.7)
	metric.Add(1, 2.7)
	metric.Add(1, 3.3)
	metric.Add(1, 1.3)
	metric.Add(1, 2.2)
	metric.Add(1, 3.1)

	assert.InDelta(t, 1.300, metric.Percentile(0), 0.001, "Percentile(0)")
	assert.InDelta(t, 2.325, metric.Percentile(25), 0.001, "Percentile(25)")
	assert.InDelta(t, 2.900, metric.Percentile(50), 0.001, "Percentile(50)")
	assert.InDelta(t, 3.250, metric.Percentile(75), 0.001, "Percentile(75)")
	assert.InDelta(t, 3.700, metric.Percentile(100), 0.001, "Percentile(100)")
}
