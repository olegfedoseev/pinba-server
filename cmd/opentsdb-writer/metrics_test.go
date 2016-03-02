package main

import (
	"testing"

	"github.com/olegfedoseev/pinba"
	"github.com/stretchr/testify/assert"
)

func TestSum(t *testing.T) {
	assert.EqualValues(t, 0, sum([]float64{}))
	assert.EqualValues(t, 0, sum([]float64{0}))
	assert.EqualValues(t, 1, sum([]float64{0, 1}))
	assert.EqualValues(t, 6, sum([]float64{1, 2, 3}))
	assert.EqualValues(t, 7, sum([]float64{1.5, 2.5, 3}))
}

func TestMetricAdd(t *testing.T) {
	metric := NewMetric("test.metric", pinba.Tags{pinba.Tag{"aaa", "val_1"}, pinba.Tag{"bbb", "val_2"}})
	assert.EqualValues(t, 0, metric.Count)

	metric.Add(1, 0.1)
	metric.Add(2, 0.2)
	metric.Add(3, 0.3)

	assert.EqualValues(t, 6, metric.Count)
	assert.EqualValues(t, 0.3, metric.Max())
	assert.EqualValues(t, 0.1, metric.Value())
}

func TestMetricPercentile(t *testing.T) {
	metric := NewMetric("test.metric", pinba.Tags{pinba.Tag{"aaa", "val_1"}, pinba.Tag{"bbb", "val_2"}})

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
