package main

import (
	"net"
)

// PinbaServer is UDP server for pinba "clients"
type PinbaServer struct {
	server *net.UDPConn
}

// NewPinbaServer verifies given address and creates PinbaServer struct
func NewPinbaServer(inAddr *string) (*PinbaServer, error) {
	addr, err := net.ResolveUDPAddr("udp4", *inAddr)
	if err != nil {
		return nil, err
	}
	sock, err := net.ListenUDP("udp4", addr)
	if err != nil {
		return nil, err
	}

	return &PinbaServer{server: sock}, nil
}

// Listen will wait for ant UDP packets and will send them to given channel
func (pinba *PinbaServer) Listen(stream chan []byte) {
	defer pinba.server.Close()
	for {
		var buf = make([]byte, 65536)
		n, _, err := pinba.server.ReadFromUDP(buf)
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
