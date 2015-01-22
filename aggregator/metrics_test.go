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
	assert.Equal(t, 1421901818, m.Timestamp)
	//assert.Equal(t, 0.0006, m.Value)
	assert.Equal(t, 1, m.Count)
	assert.Equal(t, 5, len(m.Tags))
	// assert.Equal(t, 0.0, m.Cpu)

	m, err = NewRawMetric("request", request)
	assert.Nil(t, err)
	assert.Equal(t, "request", m.Name)
	assert.Equal(t, 1421901938, m.Timestamp)
	assert.Equal(t, 123, m.Count)
	assert.Equal(t, 6, len(m.Tags))
	// assert.Equal(t, 0.01, m.Cpu)

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
	assert.Equal(t, "aaa=val_1 bbb=val_2", tags.Filter(&[]string{"aaa", "bbb"}))

	tags = Tags{Tag{"bbb", "val2"}, Tag{"aaa", "val1"}, Tag{"ccc", "val3"}}
	assert.Equal(t, "aaa=val1 bbb=val2", tags.Filter(&[]string{"aaa", "bbb"}))

	tags = Tags{Tag{"bbb", "val2"}, Tag{"aaa", ""}, Tag{"ccc", "val3"}}
	assert.Equal(t, "bbb=val2", tags.Filter(&[]string{"aaa", "bbb"}))
}
