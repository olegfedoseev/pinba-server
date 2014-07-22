package main

import (
	_ "fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPercentile(t *testing.T) {
	values0_10 := []float64{4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 0.0, 1.0, 2.0, 3.0}
	values1_10 := []float64{1.0, 2.0, 5.0, 6.0, 7.0, 3.0, 4.0, 8.0, 9.0, 10.0}

	assert.Equal(t, percentile(values0_10, 50), 4.5, "0 - 11: 50%%")
	assert.Equal(t, percentile(values0_10, 80), 7.800000000000001, "0 - 11: 80%%")
	assert.Equal(t, percentile(values0_10, 85), 8.35, "0 - 11: 85%%")
	assert.Equal(t, percentile(values0_10, 95), 9.45, "0 - 11: 95%%")
	assert.Equal(t, percentile(values0_10, 99), 9.89, "0 - 11: 99%%")

	assert.Equal(t, percentile(values1_10, 50), 5.0, "1 - 10: 50%%")
	assert.Equal(t, percentile(values1_10, 80), 8.0, "1 - 10: 80%%")
	assert.Equal(t, percentile(values1_10, 85), 8.5, "1 - 10: 85%%")
	assert.Equal(t, percentile(values1_10, 95), 9.5, "1 - 10: 95%%")
	assert.Equal(t, percentile(values1_10, 99), 9.9, "1 - 10: 99%%")
}

func TestAggregating(t *testing.T) {
	values := []float64{0.0014179999707266688, 0.001706999959424138, 0.0013460000045597553, 0.0017389999702572823, 0.001988000003620982, 0.0014179999707266688, 0.0013630000175908208, 0.001706999959424138, 0.0013460000045597553}
	result := Aggregate(values)

	assert.Equal(t, result.Max, 0.001988000003620982)
	assert.Equal(t, result.Med, 0.0014179999707266688)
	assert.Equal(t, result.Stdev, 0.00023161521512736534)

	assert.Equal(t, result.P85, 0.0017277999664656817)
	assert.Equal(t, result.P95, 0.001875949988607317)
}
