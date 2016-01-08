package client

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/olegfedoseev/pinba"
)

// PinbaRequests struct holds slice of decoded Request's and timestamp, when
// they were collected
type PinbaRequests struct {
	Timestamp int64
	Requests  []*pinba.Request
}

// NewPinbaRequests will read and decode requests for given timestamp
func NewPinbaRequests(timestamp int64, data io.Reader) (*PinbaRequests, error) {
	var buf bytes.Buffer
	var requestLen int32

	var reader bytes.Buffer
	reader.ReadFrom(data)
	length := int32(reader.Len())

	result := PinbaRequests{
		Timestamp: timestamp,
		Requests:  make([]*pinba.Request, 0),
	}

	var cnt int64
	for {
		buf.Reset()
		if err := binary.Read(&reader, binary.LittleEndian, &requestLen); err != nil {
			return nil, err
		}
		if _, err := buf.ReadFrom(io.LimitReader(&reader, int64(requestLen))); err != nil {
			return nil, err
		}

		request, err := pinba.NewRequest(buf.Bytes())
		if err != nil {
			return nil, err
		}
		cnt += 1
		result.Requests = append(result.Requests, request)

		length -= 4 + requestLen
		if length <= 0 {
			break
		}
	}
	return &result, nil
}
