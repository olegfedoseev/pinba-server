package main

import (
	"fmt"
	"log"
	"net"
	"encoding/gob"
	"time"
)

type client chan []string

type Publisher struct {
	Server  *net.TCPListener
	Data    chan []string
	clients map[string]client
	packets int
	timer   time.Duration
}

func NewPublisher(out_addr *string, data chan []string) (p *Publisher) {
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
		p.clients[addr] = make(chan []string)
		log.Printf("[Publisher] Look's like we got customer! He's from %v\n", addr)

		// Handle the connection in a new goroutine.
		go func(c net.Conn) {
			defer c.Close()
			enc := gob.NewEncoder(c)
			for {
				data := <-p.clients[addr]
				err := enc.Encode(data)
				if err != nil {
					log.Printf("[Publisher] Encode got error: '%v', closing connection.\n", err)
					delete(p.clients, addr)
					log.Printf("[Publisher] Good bye %v!", addr)
					return
				}
				log.Printf("[Publisher] Send %d to %v\n", len(data), addr)
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

			for _, c := range p.clients {
				c <- buffer
			}
			log.Printf("[Publisher] Send %d packets\n", len(buffer))
			buffer = make([]string, 0)

		// Read from channel of decoded packets
		case data := <-p.Data:
			buffer = append(buffer, data...)
		}
	}
}
