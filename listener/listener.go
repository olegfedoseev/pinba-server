package listener

import (
	"encoding/gob"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

// TCPConn wrapper for naive reconnect support
// TODO: refactor to separate generic TCPConn "reconnector"
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

func (c *connection) Connection() *net.TCPConn{
	c.Connect()

	return c.conn
}

type Listener struct {
	RawMetrics chan []*RawMetric
	conn       *connection
}

type RawMetric struct {
	Timestamp int64
	Name      string
	Count     int64
	Value     float64
	Cpu       float64
	Tags      Tags
}

func NewListener(in_addr *string) (l *Listener) {
	addr, err := net.ResolveTCPAddr("tcp4", *in_addr)
	if err != nil {
		log.Fatalf("[Listener] ResolveTCPAddr: '%v'", err)
	}

	return &Listener{
		conn:     &connection{addr: addr},
		RawMetrics: make(chan []*RawMetric, 10000),
	}
}

func (l *Listener) Start() {
	defer l.conn.Close()
	var dec *gob.Decoder
	for {
		var data = make([]string, 0)
		if dec == nil {
			dec = gob.NewDecoder(l.conn.Connection())
		}
		if err := dec.Decode(&data); err != nil {
			log.Printf("[Listener] Error on Decode: %v", err)
			// Assume connection failure, close decoder and connection, wait 5 seconds
			dec = nil
			l.conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}
		if len(data) == 0 {
			continue
		}

		start := time.Now()
		var buffer = make([]*RawMetric, len(data))
		for idx, m := range data {
			metric := strings.SplitAfterN(m, " ", 6)
			ts, err := strconv.ParseInt(strings.TrimSpace(metric[1]), 10, 32)
			if err != nil {
				log.Printf("[Listener] Error on ParseInt: %v", err)
			}
			val, err := strconv.ParseFloat(strings.TrimSpace(metric[2]), 32)
			if err != nil {
				log.Printf("[Listener] Error on ParseFloat: %v", err)
			}
			cnt, err := strconv.ParseInt(strings.TrimSpace(metric[3]), 10, 32)
			if err != nil {
				log.Printf("[Listener] Error on ParseInt: %v", err)
			}
			cpu, err := strconv.ParseFloat(strings.TrimSpace(metric[4]), 32)
			if err != nil {
				log.Printf("[Listener] Error on ParseFloat: %v", err)
			}

			var tags Tags
			if len(metric) >= 6 {
				tmp := strings.Split(metric[5], " ")
				for _, tag := range tmp {
					kv := strings.Split(tag, "=")
					if len(kv) < 2 {
						continue
					}
					tags = append(tags, Tag{kv[0], kv[1]})
				}
			}

			buffer[idx] = &RawMetric{
				Name:      strings.TrimRight(strings.TrimSpace(metric[0]), " "),
				Timestamp: ts,
				Value:     val,
				Count:     cnt,
				Cpu:       cpu,
				Tags:      tags,
			}
		}

		log.Printf("[Listener] Recive %d metrics in %v", len(buffer), time.Since(start))
		l.RawMetrics <- buffer
	}
}
