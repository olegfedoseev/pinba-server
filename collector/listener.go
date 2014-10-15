package main

import (
	"log"
	"net"
	"time"
)

type Listener struct {
	RawPackets chan RawData
	LegacyData chan []byte
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

	l = &Listener{
		server:     sock,
		RawPackets: make(chan RawData, 10000),
		LegacyData: make(chan []byte, 10000),
	}
	return l
}

func (l *Listener) Start() {
	go func() {
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
			l.LegacyData <- buf[0:rlen]
			l.RawPackets <- RawData{Data: buf[0:rlen], Timestamp: time.Now()}
		}
	}()
}
