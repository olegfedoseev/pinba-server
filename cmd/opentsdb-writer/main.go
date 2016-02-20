package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/olegfedoseev/pinba-server/client"
)

func main() {
	var (
		inAddr     = flag.String("in", "", "incoming socket")
		configFile = flag.String("config", "config.yml", "config name, default - config.yml")
	)
	flag.Parse()

	config, err := NewConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config from %v: %v", *configFile, err)
	}

	pinba, err := client.New(*inAddr, 5*time.Second, 5*time.Second)
	if err != nil {
		log.Fatalf("Failed to create pinba client: %v", err)
	}
	go pinba.Listen(config.Interval)

	metrics := make(chan []*RawMetric, 10)
	writer, err := NewWriter(config, metrics)
	if err != nil {
		log.Fatalf("Failed to create OpenTSDB writer: %v", err)
	}

	fmt.Printf("Reading from %q\n", *inAddr)
	fmt.Printf("OpenTSDB at %q\n", config.TSDBhost)
	fmt.Printf("Interval is %d\n", config.Interval)
	fmt.Printf("Prefix is %q\n", config.Prefix)
	fmt.Println()

	writer.Start(pinba.Requests)
}
