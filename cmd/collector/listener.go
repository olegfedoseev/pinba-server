package main

import (
	"net"
)

type Listener struct {
	server *net.UDPConn
}

func NewListener(inAddr *string) (*Listener, error) {
	addr, err := net.ResolveUDPAddr("udp4", *inAddr)
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
		n, _, err := l.server.ReadFromUDP(buf)
		if err != nil || n == 0 {
			continue
		}

		select {
		case stream <- buf[0:n]:
			// all good
		default:
			// chan is full
		}
	}
}
