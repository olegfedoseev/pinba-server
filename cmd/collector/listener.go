package main

import (
	"log"
	"net"
)

type Listener struct {
	server *net.UDPConn
}

func NewListener(in_addr *string) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp4", *in_addr)
	if err != nil {
		return nil, err
	}
	sock, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return nil, err
	}

	return &Listener{server: sock}, nil
}

func (l *Listener) Start(stream chan []byte) {
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

		select {
		case stream <- buf[0:rlen]:
			// all good
		default:
			// chan is full, crap
			log.Printf("[Listener] Channel is full, can't send data")
		}
	}
}
