package main

import (
	"log"
	"net"
	"time"
)

type Listener struct {
	RawPackets chan RawData
	server     *net.UDPConn
}

type RawData struct {
	Data      []byte
	Timestamp time.Time
}

func NewListener(in_addr *string) (l *Listener) {
	addr, err := net.ResolveUDPAddr("udp4", *in_addr)
	if err != nil {
		log.Fatalf("[Listener] Can't resolve address: '%v'", err)
	}
	sock, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Fatalf("[Listener] Can't open UDP socket: '%v'", err)
	}
	log.Printf("[Listener] Start listening on udp://%v\n", *in_addr)

	return &Listener{server: sock}
}

func (l *Listener) Start() (chan RawData, chan []byte) {
	result := make(chan RawData, 100)
	legacy := make(chan []byte, 100)
	go func(result chan RawData, legacy chan[] byte) {
		defer l.server.Close()
		for {
			var buf = make([]byte, 65536)
			rlen, _, err := l.server.ReadFromUDP(buf)
			if err != nil {
				log.Fatalf("[Listener] Error on sock.ReadFrom, %v", err)
			}
			if rlen == 0 {
				continue
			}
			legacy <- buf[0:rlen]
			result <- RawData{Data: buf[0:rlen], Timestamp: time.Now()}
		}
	}(result, legacy)
	return result, legacy
}
