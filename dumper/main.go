package main

import (
	"flag"
	"time"
	"fmt"
	"github.com/olegfedoseev/pinba-server/listener"
	"runtime"
	"strings"
)

type Metric struct {
	Timestamp int64
	Name      string
	Count     int64
	Value     float64
	Cpu       float64
	Tags      string
}

var (
	in_addr   = flag.String("in", "", "incoming socket")
	dump_type = flag.String("type", "php", "incoming socket")

	server_name = flag.String("server", "", "server")
	hostname    = flag.String("hostname", "", "hostname")
	script_name = flag.String("script_name", "", "script_name")

	requests = flag.Bool("requests", false, "show requests")
	timers   = flag.Bool("timers", false, "show timers")
	pinba    = flag.Bool("pinba", false, "show pinba")
)

// dumper --in=tcp://172.16.5.130:5003 --server=example.com

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(4)

	listener := listener.NewListener(in_addr)
	go listener.Start()

	for {
		metrics := <-listener.RawMetrics

		for _, metric := range metrics {
			if *hostname != "" && !strings.Contains(metric.Tags, fmt.Sprintf("host=%s", *hostname)) {
				continue
			}
			if *server_name != "" && !strings.Contains(metric.Tags, fmt.Sprintf("server=%s", *server_name)) {
				continue
			}
			if *script_name != "" && !strings.Contains(metric.Tags, fmt.Sprintf("script=%s", *script_name)) {
				continue
			}

			if metric.Name == "request" && !*requests {
				continue
			}

			if metric.Name == "timer" && !*timers {
				continue
			}

			if *pinba {
				fmt.Printf("%d %s %3.4f\n", metric.Timestamp, metric.Name, metric.Value)
				continue
			}

			if *dump_type == "nginx" {
				fmt.Printf("[%v] %3.4f [%s]\n", time.Unix(metric.Timestamp, 0).Format("2006-01-02 15:04:05"), metric.Value, metric.Tags)
			} else {
				fmt.Printf("Metric name: %s\n", metric.Name)
				fmt.Printf("[%d: Value %3.4f, Count: %d, CPU: %3.4f] Tags %s\n", metric.Timestamp, metric.Value, metric.Count, metric.Cpu, metric.Tags)
				fmt.Printf("\n")
			}

		}
	}
}
