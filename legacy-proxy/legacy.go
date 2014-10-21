package main

import (
	"bytes"
	"compress/zlib"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"log"
	"time"
)

// --in=0.0.0.0:30002 --out=tcp://0.0.0.0:5005
func LegacySender(data chan []byte) {
	context, _ := zmq.NewContext()
	defer context.Close()
	publisher, err := context.NewSocket(zmq.PUB)
	if err != nil {
		log.Fatalf("Error on context.NewSocket(zmq.PUB), %v", err)
	}
	defer publisher.Close()
	publisher.Bind("tcp://0.0.0.0:5005")
	publisher.SetHWM(1)

	var buffer bytes.Buffer
	counter := 0

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case now := <-ticker.C:

			if counter > 0 {
				t := time.Now()
				var b bytes.Buffer
				w := zlib.NewWriter(&b)
				w.Write(buffer.Bytes()[:len(buffer.Bytes())-4])
				w.Close()
				publisher.Send([]byte(fmt.Sprintf("%d\n%s", now.Unix(), b.Bytes())), 0)
				log.Printf("[Legacy] %v requests, %v bytes in %v\n", counter, len(buffer.Bytes()), time.Since(t))
			}
			buffer = *bytes.NewBuffer([]byte{})
			counter = 0
		case d := <-data:
			buffer.Write(d)
			buffer.Write([]byte{0xa, 0x2d, 0x2d, 0xa}) // Delimeter
			counter += 1
		}
	}
}
