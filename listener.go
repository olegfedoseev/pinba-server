package main

import (
	"log"
	"net"
	"strings"
	"time"
)

/*
	UDP Listener that collects every packet from pinba and sends them every
	second to channel for publishing.
*/
type Listener struct {
	Server  *net.UDPConn
	Out     chan<- []string
	packets int
	timer   time.Duration
}

func NewListener(in_addr *string, out chan<- []string) (l *Listener) {
	addr, err := net.ResolveUDPAddr("udp4", *in_addr)
	if err != nil {
		log.Printf("[Listener] Error on net.ResolveUDPAddr, %v", err)
		panic(err)
	}
	sock, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Printf("[Listener] Error on net.ListenUDP, %v", err)
		panic(err)
	}
	log.Printf("[Listener] Start listening on udp://%v\n", *in_addr)

	l = &Listener{
		Server: sock,
		Out:    out,
	}
	return l
}

func (l *Listener) reciver(buffer chan<- string) {
	defer l.Server.Close()
	for {
		var buf = make([]byte, 65536)
		rlen, _, err := l.Server.ReadFromUDP(buf)
		if err != nil {
			log.Printf("[Listener] Error on sock.ReadFrom, %v", err)
			panic(err)
		}
		if rlen == 0 {
			continue
		}

		l.packets++
		go func(data []byte) {
			start := time.Now()
			metrics, err := Decode(time.Now().Unix(), data)
			if err != nil {
				log.Printf("[Listener] Error decoding pb packet: %v", err)
				//log.Printf("var data = %#v \n", data)
				return
			}

			buffer <- strings.Join(metrics, "")
			l.timer += time.Now().Sub(start)
		}(buf[0:rlen])
	}
}

func (l *Listener) Run() {

	buffer := make(chan string, 1000)
	result := make([]string, 0)

	ticks := make(chan time.Time)
	go func(ticks chan<- time.Time) {
		for now := range time.Tick(time.Second) {
			ticks <- now
		}
	}(ticks)

	go l.reciver(buffer)
	var idle_time time.Duration
	for {
		select {
		case now := <-ticks:
			if l.packets == 0 {
				idle_time += time.Second
				log.Printf("[Listener] No packets for %v!\n", idle_time)
				continue
			}
			l.Out <- result

			log.Printf("[Listener] Received %v: %d/%v (%v)\n", now.Unix(), l.packets, len(result), l.timer)
			if l.timer > time.Second {
				log.Printf("[Listener] Processing took more than 1 second (%v)!\n", l.timer)
			}

			l.timer = 0
			l.packets = 0
			idle_time = 0
			result = make([]string, 0)
		case data := <-buffer:
			result = append(result, data)
		}
	}
}
