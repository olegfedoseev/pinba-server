package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"net"
)

type Listener struct {
	RawData chan []byte
	conn    *net.TCPConn
}

func NewListener(in_addr *string) (l *Listener) {
	addr, err := net.ResolveTCPAddr("tcp4", *in_addr)
	if err != nil {
		log.Fatalf("[Listener] ResolveTCPAddr: '%v'", err)
	}

	// TODO: implement reconnect
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Fatalf("[Listener] DialTCP: '%v'", err)
	}
	conn.SetKeepAlive(true)
	log.Printf("[Listener] Start listening on tcp://%v\n", *in_addr)

	l = &Listener{
		conn:    conn,
		RawData: make(chan []byte, 10000),
	}
	return l
}

func (l *Listener) Start() {
	defer l.conn.Close()
	for {
		var ts int32
		var length int32
		if err := binary.Read(l.conn, binary.LittleEndian, &length); err != nil {
			log.Printf("[Listener] binary.Read of length failed:", err)
			break
		}
		if err := binary.Read(l.conn, binary.LittleEndian, &ts); err != nil {
			log.Printf("[Listener] binary.Read of timestamp failed:", err)
			break
		}

		data := make([]byte, 0, length)
		buf := make([]byte, 102400)
		for {
			n, err := l.conn.Read(buf)
			length -= int32(n)
			if err != nil {
				if err != io.EOF {
					log.Printf("[Listener] Read error:", err)
					break
				}
				break
			}
			data = append(data, buf[:n]...)

			// We read enough
			if length == 0 {
				break
			}
		}

		r, err := zlib.NewReader(bytes.NewBuffer(data))
		if err != nil {
			log.Printf("[Listener] Read error:", err)
			break
		}
		result, err := ioutil.ReadAll(r)
		if err != nil {
			log.Printf("[Listener] Read error:", err)
			break
		}
		r.Close()

		b := bytes.NewBuffer(result)
		data_length := len(result)
		counter := 0
		for {
			var part_length int32
			if err := binary.Read(b, binary.LittleEndian, &part_length); err != nil {
				log.Printf("[Listener] binary.Read of length failed:", err)
				break
			}
			data_length -= 4 + int(part_length)

			part := make([]byte, part_length)
			if _, err := b.Read(part); err != nil {
				log.Printf("[Listener] Read error:", err)
			}
			l.RawData <- part
			counter += 1
			if data_length == 0 {
				break
			}
		}
		log.Printf("[Listener] Got %d packets for %d timestamp", counter, ts)
	}
}
