package client

import (
	"bytes"
	"log"
	"net"
	"time"
)

// rawRequests is struct to pass raw pinba requests along with timestamp
type rawRequests struct {
	Timestamp int64
	Data      []byte
}

// Client is net.Conn wrapper for reading data from pinba-collector
type Client struct {
	Requests chan *PinbaRequests

	serverAddr     string
	serverConn     net.Conn
	connectTimeout time.Duration
	readTimeout    time.Duration

	stream chan rawRequests
}

// New validates given address and creates new Client
func New(addr string, connectTimeout, readTimeout time.Duration) (*Client, error) {
	_, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return nil, err
	}

	client := &Client{
		Requests:       make(chan *PinbaRequests, 10),
		serverAddr:     addr,
		connectTimeout: connectTimeout,
		readTimeout:    readTimeout,
	}
	return client, nil
}

// Listen will read all data from socket into buffer, and will flush it
// to internal channel for processing with given interval
func (c *Client) Listen(interval int64) {
	c.serverConn = mustConnect(c.serverAddr, c.connectTimeout)
	defer c.serverConn.Close()

	go c.decode()

	var message serverMessage
	var buffer bytes.Buffer

	lastFlush := time.Now().Unix()
	c.stream = make(chan rawRequests, 10)

	for {
		c.serverConn.SetReadDeadline(time.Now().Add(c.readTimeout))
		if err := message.ReadFrom(c.serverConn); err != nil {
			log.Printf("[ERROR] Failed to read message: (%#v) %v", err, err)
			c.serverConn = mustConnect(c.serverAddr, c.connectTimeout)
			continue
		}
		log.Printf("[INFO] Read message for %v / %v (%v bytes)",
			time.Unix(int64(message.Timestamp), 0).Format("15:04:05"),
			message.Timestamp,
			message.Data.Len(),
		)

		// Append message to buffer
		buffer.ReadFrom(&message.Data)

		// If it's time to flush buffer
		if message.Timestamp%interval == 0 || message.Timestamp-lastFlush > interval {
			select {
			case c.stream <- rawRequests{message.Timestamp, buffer.Bytes()}:
				// Sending buffer to processing
			default:
				log.Printf("[WARN] Stream channel is full! Skipping requests for %v", message.Timestamp)
			}
			lastFlush = message.Timestamp
			buffer.Reset()
		}
	}

	close(c.stream)
}

// decode get data from stream channel and decode it from []byte to pinba.Request
// and sends its result to Client.Requests channel
func (c *Client) decode() {
	for {
		select {
		case data := <-c.stream:

			t := time.Now()
			requests, err := NewPinbaRequests(data.Timestamp, bytes.NewReader(data.Data))
			if err != nil {
				log.Printf("[ERROR] Failed to unmarshal request for %v: %v", data.Timestamp, err)
				continue
			}
			log.Printf("[INFO] Decoded %v requests for %v in %v", len(requests.Requests), data.Timestamp, time.Since(t))

			select {
			case c.Requests <- requests:
				// Sending requests to clients client
			default:
				log.Printf("[WARN] Requests channel is full! Skipping requests for %v", data.Timestamp)
			}
		}
	}
}

// mustConnect try to connect to server, and if failed will retry every 5 seconds
// TODO: move 5 seconds constant to Clients property?
func mustConnect(addr string, timeout time.Duration) net.Conn {
	for {
		conn, err := net.DialTimeout("tcp", addr, timeout)
		if err != nil {
			log.Printf("[WARN] Can't connect to %v: %v", addr, err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("[INFO] Connected to tcp://%v", conn.RemoteAddr())
		return conn
	}
}
