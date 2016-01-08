package client

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"io"
)

// serverMessage is struct to read and "decode" pinba-collector (server) messages
type serverMessage struct {
	Timestamp int64
	Data      bytes.Buffer

	length int32
}

// ReadFrom will read message from given io.Reader and "extract" from it
// timestamp and raw byte data of pinba requests for this timestamp
func (message *serverMessage) ReadFrom(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &message.length); err != nil {
		return err
	}
	var ts int32
	if err := binary.Read(r, binary.LittleEndian, &ts); err != nil {
		return err
	}
	message.Timestamp = int64(ts)
	zdata, err := zlib.NewReader(io.LimitReader(r, int64(message.length)))
	if err != nil {
		return err
	}
	zdata.Close()

	if _, err := message.Data.ReadFrom(zdata); err != nil {
		return err
	}
	return nil
}
