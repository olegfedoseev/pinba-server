package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"time"
)

type client chan []byte

type Publisher struct {
	Server  *net.TCPListener
	clients map[string]client
	packets int
	timer   time.Duration
}

func NewPublisher(out_addr *string) (p *Publisher) {
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
		clients: clients,
	}
	return p
}

func (p *Publisher) sender() {
	defer p.Server.Close()
	for {
		// Wait for a connection.
		conn, err := p.Server.AcceptTCP()
		if err != nil {
			log.Fatal(err)
		}

		addr := fmt.Sprintf("%v", conn.RemoteAddr())
		p.clients[addr] = make(chan []byte, 10)
		log.Printf("[Publisher] Look's like we got customer! He's from %v", addr)

		// Handle the connection in a new goroutine.
		go func(c *net.TCPConn) {
			defer c.Close()
			c.SetNoDelay(false)

			for {
				data := <-p.clients[addr]
				t := time.Now()

				var b bytes.Buffer
				w := zlib.NewWriter(&b)
				w.Write(data)
				w.Close()

				var length int32 = int32(b.Len())
				var ts int32 = int32(time.Now().Unix())

				header := new(bytes.Buffer)
				if err := binary.Write(header, binary.LittleEndian, length); err != nil {
					fmt.Printf("Failed to Write header length: %v", err)
				}
				if err := binary.Write(header, binary.LittleEndian, ts); err != nil {
					fmt.Printf("Faield to Write header timestamp: %v", err)
				}

				c.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := c.Write(header.Bytes()); err != nil {
					log.Printf("[Publisher] Failed to Write: '%v', closing connection.", err)
					break
				}
				n, err := c.Write(b.Bytes())
				if err != nil {
					log.Printf("[Publisher] Failed to Write: '%v', closing connection.", err)
					break
				}
				c.SetWriteDeadline(time.Time{}) // No timeout
				log.Printf("[Publisher] Writen %d bytes in %v", n, time.Since(t))
			}
			delete(p.clients, addr)
			log.Printf("[Publisher] Goodbye %v!", addr)
		}(conn)
	}
}

func (p *Publisher) Start(stream chan []byte) {
	go p.sender()

	var buffer bytes.Buffer
	idleTime := time.Now()
	ticker := time.NewTicker(time.Second)
	counter := 0
	for {
		select {
		case now := <-ticker.C:
			if counter == 0 {
				log.Printf("[Publisher] No packets for %.f sec (since %v)!\n",
					time.Now().Sub(idleTime).Seconds(), idleTime.Format("15:04:05"))
				continue
			}
			idleTime = now

			if len(p.clients) > 0 {
				for _, c := range p.clients {
					// TODO: send time with data
					if len(c) == 10 {
						close(c) // clients channel is full, looks like it's dead
						log.Printf("[Publisher] Close client connection, too slow")
						continue
					}
					c <- buffer.Bytes()
				}
				log.Printf("[Publisher] Send %d packets to %d clients\n", counter, len(p.clients))
			} else {
				log.Printf("[Publisher] Got %d packets, but no clients to send to!\n", counter)
			}

			buffer.Reset()
			counter = 0

		// Read from channel of decoded packets
		case data := <-stream:
			n := int32(len(data))
			if err := binary.Write(&buffer, binary.LittleEndian, n); err != nil {
				fmt.Printf("Failed to write data length: %v", err)
			}
			buffer.Write(data)
			counter += 1
		}
	}
}
