package main

import (
	"fmt"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type MetricsConfig struct {
	Name        string   `yaml:"name"`
	Tags        []string `yaml:"tags"`
	Type        string   `yaml:"type"`
	ReqiredTags []string `yaml:"required"`
	CPUTime     bool     `yaml:"cpu"`
}

type Config struct {
	Metrics  []MetricsConfig `yaml:"metrics"`
	Prefix   string          `yaml:"prefix"`
	Interval int64           `yaml:"interval"`
	TSDBhost string          `yaml:"tsdb_host"`
}

func NewConfig(filename string) (*Config, error) {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil, fmt.Errorf("no such file: %s", filename)
	}

	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("reading file %s failed: %v", filename, err)
	}

	config := Config{}
	if err := yaml.Unmarshal(file, &config); err != nil {
		return nil, fmt.Errorf("unmarshal failed: %v", err)
	}
	return &config, nil
}
