package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"net"
	"time"
)

// TCPConn wrapper for naive reconnect support
type connection struct {
	addr *net.TCPAddr
	conn *net.TCPConn
}

func (c *connection) Connect() (err error) {
	if c.conn != nil {
		return nil
	}
	if c.conn, err = net.DialTCP("tcp", nil, c.addr); err != nil {
		return err
	}
	c.conn.SetKeepAlive(true)

	log.Printf("[Connection] Connected to tcp://%v", c.addr)
	return nil
}

func (c *connection) Close() {
	if c.conn == nil {
		return
	}
	c.conn.Close()
	c.conn = nil

	log.Printf("[Connection] Close connection to tcp://%v", c.addr)
}

func (c *connection) Read() (ts *int32, data *[]byte, err error) {
	if err = c.Connect(); err != nil {
		log.Printf("[Connection] Can't connect: %v", err)
		return nil, nil, err
	}

	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var length int32
	if err := binary.Read(c.conn, binary.LittleEndian, &length); err != nil {
		log.Printf("[Connection] Failed to read 'length': %v", err)
		c.Close()
		return nil, nil, err
	}
	var timestamp int32
	if err := binary.Read(c.conn, binary.LittleEndian, &timestamp); err != nil {
		log.Printf("[Connection] Failed to read 'timestamp': %v", err)
		c.Close()
		return nil, nil, err
	}

	buffer := make([]byte, 0, length)
	tmp := make([]byte, 102400)
	for {
		n, err := c.conn.Read(tmp)
		length -= int32(n)
		if err != nil {
			if err != io.EOF {
				log.Printf("[Connection] Failed to read data: %v", err)
				c.Close()
				return nil, nil, err
			}
			c.Close()
			return nil, nil, err
		}
		buffer = append(buffer, tmp[:n]...)

		// We read enough
		if length == 0 {
			break
		}
	}
	c.conn.SetReadDeadline(time.Time{}) // No timeout

	return &timestamp, &buffer, nil
}

type Listener struct {
	RawMetrics chan RawData
	conn       *connection
}

type RawData struct {
	Data      []byte
	Timestamp int32
}

func NewListener(in_addr *string) (l *Listener) {
	addr, err := net.ResolveTCPAddr("tcp4", *in_addr)
	if err != nil {
		log.Fatalf("[Listener] ResolveTCPAddr: '%v'", err)
	}

	return &Listener{
		conn:       &connection{addr: addr},
		RawMetrics: make(chan RawData, 20000),
	}
}

func (l *Listener) Start() {
	defer l.conn.Close()
	for {
		ts, data, err := l.conn.Read()
		if err != nil {
			log.Printf("[Listener] Failed to read from socket: %v", err)
			time.Sleep(5 * time.Second) // wait 5 sec till next try
			continue
		}

		r, err := zlib.NewReader(bytes.NewBuffer(*data))
		if err != nil {
			log.Printf("[Listener] Failed to create zlib reader: %v", err)
			continue
		}
		result, err := ioutil.ReadAll(r)
		if err != nil {
			log.Printf("[Listener] Failed to read from zlib reader: %v", err)
			continue
		}
		r.Close()

		b := bytes.NewBuffer(result)
		data_length := len(result)
		counter := 0
		for {
			var part_length int32
			if err := binary.Read(b, binary.LittleEndian, &part_length); err != nil {
				log.Printf("[Listener] Failed to read 'length': %v", err)
				continue
			}
			data_length -= 4 + int(part_length)

			part := make([]byte, part_length)
			if _, err := b.Read(part); err != nil {
				log.Printf("[Listener] Failed to read data: %v", err)
				continue
			}
			l.RawMetrics <- RawData{Data: part, Timestamp: *ts}
			counter += 1
			if data_length == 0 {
				break
			}
		}
		log.Printf("[Listener] Got %d packets for %v", counter, time.Unix(int64(*ts), 0).Format("15:04:05"))
	}
}
