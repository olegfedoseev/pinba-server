package main

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

type client chan []byte

type Publisher struct {
	Server  *net.TCPListener
	Inbound <-chan []string
	Clients map[string]client
	gzip    bool
	packets int
	timer   time.Duration
}

func NewPublisher(out_addr *string, inbound <-chan []string, gzip bool) (p *Publisher) {
	addr, err := net.ResolveTCPAddr("tcp", *out_addr)
	if err != nil {
		log.Fatalf("[Publisher] Can't resolve address: '%v'", err)
	}
	sock, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatalf("[Publisher] Can't open TCP socket: '%v'", err)
	}
	log.Printf("[Publisher] Start listening on tcp://%v\n", *out_addr)

	clients := make(map[string]client, 0)
	p = &Publisher{
		Server:  sock,
		Inbound: inbound,
		Clients: clients,
		gzip:    gzip,
	}
	return p
}

func (p *Publisher) sender() {
	defer p.Server.Close()
	for {
		// Wait for a connection.
		conn, err := p.Server.Accept()
		if err != nil {
			log.Fatal(err)
		}

		addr := fmt.Sprintf("%v", conn.RemoteAddr())
		p.Clients[addr] = make(chan []byte)
		log.Printf("[Publisher] Look's like we got customer! He's from %v\n", addr)

		// Handle the connection in a new goroutine.
		go func(c net.Conn) {
			defer c.Close()
			for {
				if _, err := c.Write(<-p.Clients[addr]); err != nil {
					log.Printf("[Publisher] net.Conn.Write got error: '%v', closing connection.\n", err)
					delete(p.Clients, addr)
					log.Printf("[Publisher] Good bye %v!", addr)
					return
				}
			}
		}(conn)
	}
}

func (p *Publisher) Run() {
	go p.sender()

	for {
		data := <-p.Inbound
		if len(data) == 0 {
			log.Printf("[Publisher] Nothing to send!\n")
			continue
		}
		if len(p.Clients) == 0 {
			log.Printf("[Publisher] No clients to send to!\n")
			continue
		}

		start := time.Now()
		var result []byte
		if p.gzip {
			var b bytes.Buffer
			w := zlib.NewWriter(&b)
			w.Write([]byte(strings.Join(data, "")))
			w.Close()
			result = b.Bytes()
		} else {
			result = []byte(strings.Join(data, ""))
		}

		for _, c := range p.Clients {
			c <- result
		}
		timer := time.Now().Sub(start)
		log.Printf("[Publisher] Send %v bytes in %v\n", len(result), timer)
	}
}
