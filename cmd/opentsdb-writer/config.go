package main

import (
	"fmt"
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type writerConfig struct {
	Metrics []MetricsSettings `yaml:"metrics"`

	Prefix     string `yaml:"prefix"`
	Interval   int64  `yaml:"interval"`
	Workers    int    `yaml:"workers"`
	BatchSize  int    `yaml:"batch_size"`
	BufferSize int    `yaml:"buffer_size"`
	TSDB       struct {
		Host    string `yaml:"host"`
		Timeout int64  `yaml:"timeout"`
	} `yaml:"tsdb"`
}

func getConfig(filename string) (*writerConfig, error) {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading file %s failed: %v", filename, err)
	}

	config := writerConfig{}
	if err := yaml.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %v", err)
	}
	return &config, nil
}
