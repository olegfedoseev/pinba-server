package main

/*

!!!! https://github.com/rcrowley/go-metrics/blob/master/influxdb/influxdb.go

*/

import (
	"flag"
	"log"
	"runtime"
	"time"
)

var (
	in_addr     = flag.String("in", "", "incoming socket")
	kairosdb_addr = flag.String("kairosdb", "", "address of kairosdb instance")
	influxdb_addr = flag.String("influxdb", "", "address of influxdb instance")

	cpu         = flag.Int("cpu", 1, "how much cores to use")
	interval    = flag.Int("interval", 1, "interval of aggregation in seconds")
)

func main() {
	flag.Parse()

	log.Printf("Pinba aggregator reading from %s\n", *in_addr)
	log.Printf("Using %d/%d CPU\n", *cpu, runtime.NumCPU())
	runtime.GOMAXPROCS(*cpu)

	if *kairosdb_addr == "" &&  *influxdb_addr == "" {
		log.Fatal("No writer specified!\n")
	}
	if *kairosdb_addr != "" &&  *influxdb_addr != "" {
		log.Fatal("You can't use both writers!\n")
	}

	ts := time.Now().Unix()
	wait := ts - ts%int64(*interval) + int64(*interval) - ts
	log.Printf("[Aggregator] Starting after %vsec", wait)
	time.Sleep(time.Duration(wait) * time.Second)

	listener := NewListener(in_addr)
	go listener.Start()

	aggregator := NewAggregator(listener.RawMetrics)
	go aggregator.Start(int64(*interval))

	if *kairosdb_addr != "" {
		log.Printf("Writing to KairosDB at %s\n", *kairosdb_addr)
		writer := NewKairosWriter(kairosdb_addr, aggregator.Metrics)
		writer.Start()
	}

	if *influxdb_addr != "" {
		log.Printf("Writing to InfluxDB at %s\n", *influxdb_addr)
		writer := NewInfluxWriter(influxdb_addr, aggregator.Metrics)
		writer.Start()
	}
}
