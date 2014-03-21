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
	Data    chan string
	clients map[string]client
	gzip    bool
	packets int
	timer   time.Duration
}

func NewPublisher(out_addr *string, data chan string, gzip bool) (p *Publisher) {
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
		Data:    data,
		clients: clients,
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
		p.clients[addr] = make(chan []byte) // http://golang.org/doc/faq#atomic_maps
		log.Printf("[Publisher] Look's like we got customer! He's from %v\n", addr)

		// Handle the connection in a new goroutine.
		go func(c net.Conn) {
			defer c.Close()
			for {
				if _, err := c.Write(<-p.clients[addr]); err != nil {
					log.Printf("[Publisher] net.Conn.Write got error: '%v', closing connection.\n", err)
					delete(p.clients, addr)
					log.Printf("[Publisher] Good bye %v!", addr)
					return
				}
			}
		}(conn)
	}
}

func (p *Publisher) Start() {
	go p.sender()

	buffer := make([]string, 0)
	idle_since := time.Now()
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case now := <-ticker.C:
			if len(buffer) == 0 {
				log.Printf("[Publisher] No packets for %.f sec (since %v)!\n",
					time.Now().Sub(idle_since).Seconds(), idle_since.Format("15:04:05"))
				continue
			}
			idle_since = now
			log.Printf("[Publisher] Received %v: %d\n", now.Unix(), len(buffer))

			if len(p.clients) == 0 {
				log.Printf("[Publisher] No clients to send to!\n")
				buffer = make([]string, 0)
				continue
			}

			start := time.Now()
			var result []byte
			if p.gzip {
				var b bytes.Buffer
				w := zlib.NewWriter(&b)
				w.Write([]byte(strings.Join(buffer, "")))
				w.Close()
				result = b.Bytes()
			} else {
				result = []byte(strings.Join(buffer, ""))
			}

			for _, c := range p.clients {
				c <- result
			}
			timer := time.Now().Sub(start)
			log.Printf("[Publisher] Send %v bytes in %v\n", len(buffer), timer)

			//l.timer = 0

			buffer = make([]string, 0)

		// Read from channel of decoded packets
		case data := <-p.Data:
			buffer = append(buffer, data)
		}
	}
}
