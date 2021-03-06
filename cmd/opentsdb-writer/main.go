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
		tsdbAddr   = flag.String("tsdb", "", "tsdb host:port")
		configFile = flag.String("config", "config.yml", "config name, default - config.yml")
	)
	flag.Parse()

	config, err := getConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config from %v: %v", *configFile, err)
	}
	// Overwrite OpenTSDB host in config
	if *tsdbAddr != "" {
		config.TSDB.Host = *tsdbAddr
	}

	pinba, err := client.New(*inAddr, 5*time.Second, 5*time.Second)
	if err != nil {
		log.Fatalf("Failed to create pinba client: %v", err)
	}
	go pinba.Listen(config.Interval)

	writer, err := NewWriter(config)
	if err != nil {
		log.Fatalf("Failed to create OpenTSDB writer: %v", err)
	}

	fmt.Printf("Reading from %q\n", *inAddr)
	fmt.Printf("OpenTSDB at %q\n", config.TSDB.Host)
	fmt.Printf("Interval is %d\n", config.Interval)
	fmt.Printf("Prefix is %q\n", config.Prefix)
	fmt.Println()

	writer.Start(pinba.Requests)
}
