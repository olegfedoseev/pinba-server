package main

import (
	"log"
	"net"
	"time"
)

type clientChan chan []byte

type Publisher struct {
	Server  *net.TCPListener
	clients map[string]clientChan
	packets int
	timer   time.Duration
}

func NewPublisher(outAddr *string) (*Publisher, error) {
	addr, err := net.ResolveTCPAddr("tcp", *outAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}

	clients := make(map[string]clientChan, 0)
	p := &Publisher{
		Server:  listener,
		clients: clients,
	}
	return p, nil
}

func (p *Publisher) sender() {
	defer p.Server.Close()
	for {
		// Wait for a connection.
		conn, err := p.Server.AcceptTCP()
		if err != nil {
			log.Fatal(err)
		}

		p.clients[conn.RemoteAddr().String()] = make(chan []byte, 10)
		log.Printf("Look's like we got customer! He's from %v", conn.RemoteAddr())

		// Handle the connection in a new goroutine.
		go func(client *net.TCPConn) {
			defer client.Close()
			client.SetNoDelay(false)

			for {
				data := <-p.clients[client.RemoteAddr().String()]

				client.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := client.Write(data); err != nil {
					log.Printf("Failed to Write: '%v', closing connection", err)
					break
				}
				client.SetWriteDeadline(time.Time{}) // No timeout
			}
			delete(p.clients, client.RemoteAddr().String())
			log.Printf("Goodbye %v!", client.RemoteAddr())
		}(conn)
	}
}

func (p *Publisher) Start(stream chan []byte) {
	go p.sender()

	var packet Packet

	idleTime := time.Now()
	ticker := time.NewTicker(time.Second)
	for {
		select {
		// Read from channel of decoded packets
		case data := <-stream:
			if err := packet.AddRequest(data); err != nil {
				log.Printf("Failed to add request: %v", err)
			}

		case now := <-ticker.C:
			if packet.Count == 0 {
				log.Printf("No packets for %.f sec (since %v)!\n",
					time.Now().Sub(idleTime).Seconds(), idleTime.Format("15:04:05"))
				continue
			}
			idleTime = now

			t := time.Now()
			data, err := packet.Get(now)
			if err != nil {
				log.Printf("Failed to prepare packet: %v", err)
				packet.Reset()
				continue
			}
			log.Printf("Prepared packet of %v requests in %v", packet.Count, time.Since(t))

			for _, c := range p.clients {
				if len(c) == cap(c) {
					close(c) // clients channel is full, looks like it's dead
					log.Printf("Close client connection - too slow")
					continue
				}
				c <- data
			}
			packet.Reset()
		}
	}
}
