package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"time"
)

// TODO: maybe combine it with client's ServerMessage and PinbaRequests?

// Packet is struct for "encoding" bunch of binary pinba packets
type Packet struct {
	payload bytes.Buffer
	Count   int64
}

// Reset will reset (duh!) underlying buffer and counter
func (packet *Packet) Reset() {
	packet.payload.Reset()
	packet.Count = 0
}

// AddRequest add given byte slice as another requests with it's lentgh to buffer
func (packet *Packet) AddRequest(data []byte) error {
	n := int32(len(data))
	if err := binary.Write(&packet.payload, binary.LittleEndian, n); err != nil {
		return err
	}
	packet.payload.Write(data)
	packet.Count += 1
	return nil
}

// Get will compress payload, format "header" with given timestamp and return
// byte slice ready to be send over the wire
func (packet *Packet) Get(timestamp time.Time) ([]byte, error) {
	var payload bytes.Buffer
	zw := zlib.NewWriter(&payload)
	packet.payload.WriteTo(zw)
	zw.Close()

	length := int32(payload.Len())
	ts := int32(timestamp.Unix())

	var result bytes.Buffer
	if err := binary.Write(&result, binary.LittleEndian, length); err != nil {
		return []byte{}, err
	}
	if err := binary.Write(&result, binary.LittleEndian, ts); err != nil {
		return []byte{}, err
	}
	if _, err := result.Write(payload.Bytes()); err != nil {
		return []byte{}, err
	}
	return result.Bytes(), nil
}
