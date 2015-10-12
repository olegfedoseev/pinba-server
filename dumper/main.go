package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/golang/protobuf/proto"
	"log"
	"net"
	"strconv"
)

type Request struct {
	Hostname         *string    `protobuf:"bytes,1,req,name=hostname" json:"hostname,omitempty"`
	ServerName       *string    `protobuf:"bytes,2,req,name=server_name" json:"server_name,omitempty"`
	ScriptName       *string    `protobuf:"bytes,3,req,name=script_name" json:"script_name,omitempty"`
	RequestCount     *uint32    `protobuf:"varint,4,req,name=request_count" json:"request_count,omitempty"`
	DocumentSize     *uint32    `protobuf:"varint,5,req,name=document_size" json:"document_size,omitempty"`
	MemoryPeak       *uint32    `protobuf:"varint,6,req,name=memory_peak" json:"memory_peak,omitempty"`
	RequestTime      *float32   `protobuf:"fixed32,7,req,name=request_time" json:"request_time,omitempty"`
	RuUtime          *float32   `protobuf:"fixed32,8,req,name=ru_utime" json:"ru_utime,omitempty"`
	RuStime          *float32   `protobuf:"fixed32,9,req,name=ru_stime" json:"ru_stime,omitempty"`
	TimerHitCount    []uint32   `protobuf:"varint,10,rep,name=timer_hit_count" json:"timer_hit_count,omitempty"`
	TimerValue       []float32  `protobuf:"fixed32,11,rep,name=timer_value" json:"timer_value,omitempty"`
	TimerTagCount    []uint32   `protobuf:"varint,12,rep,name=timer_tag_count" json:"timer_tag_count,omitempty"`
	TimerTagName     []uint32   `protobuf:"varint,13,rep,name=timer_tag_name" json:"timer_tag_name,omitempty"`
	TimerTagValue    []uint32   `protobuf:"varint,14,rep,name=timer_tag_value" json:"timer_tag_value,omitempty"`
	Dictionary       []string   `protobuf:"bytes,15,rep,name=dictionary" json:"dictionary,omitempty"`
	Status           *uint32    `protobuf:"varint,16,opt,name=status" json:"status,omitempty"`
	MemoryFootprint  *uint32    `protobuf:"varint,17,opt,name=memory_footprint" json:"memory_footprint,omitempty"`
	Requests         []*Request `protobuf:"bytes,18,rep,name=requests" json:"requests,omitempty"`
	Schema           *string    `protobuf:"bytes,19,opt,name=schema" json:"schema,omitempty"`
	TagName          []uint32   `protobuf:"varint,20,rep,name=tag_name" json:"tag_name,omitempty"`
	TagValue         []uint32   `protobuf:"varint,21,rep,name=tag_value" json:"tag_value,omitempty"`
	TimerUtime       []float32  `protobuf:"fixed32,22,rep,name=timer_ru_utime" json:"timer_ru_utime,omitempty"`
	TimerStime       []float32  `protobuf:"fixed32,23,rep,name=timer_ru_stime" json:"timer_ru_stime,omitempty"`
	XXX_unrecognized []byte     `json:"-"`
}

func (m *Request) Reset() {
	*m = Request{}
}

func (m *Request) String() string {
	return proto.CompactTextString(m)
}

func (*Request) ProtoMessage() {

}

var (
	in_addr = flag.String("in", "0.0.0.0:30002", "incoming socket")
)

func main() {
	flag.Parse()

	addr, err := net.ResolveUDPAddr("udp4", *in_addr)
	if err != nil {
		log.Fatalf("Can't resolve address: '%v'", err)
	}

	sock, err := net.ListenUDP("udp4", addr)
	if err != nil {
		log.Fatalf("Can't open UDP socket: '%v'", err)
	}

	log.Printf("Start listening on udp://%v\n", *in_addr)

	defer sock.Close()

	for {
		var buf = make([]byte, 65536)
		rlen, _, err := sock.ReadFromUDP(buf)
		if err != nil {
			log.Fatalf("Error on sock.ReadFrom, %v", err)
		}
		if rlen == 0 {
			continue
		}

		request := &Request{}
		proto.Unmarshal(buf[0:rlen], request)
		fmt.Printf("%15s %30s: %3.2f %s\n",
			*request.ServerName,
			*request.ScriptName,
			*request.RequestTime,
			request.Tags(),
		)
		for _, timer := range GetTimers(request) {
			fmt.Printf("\t%s\n", timer)
		}
	}
}

func (request *Request) Tags() string {
	var tags bytes.Buffer
	if request.Status != nil {
		tags.WriteString(" status=")
		tags.WriteString(strconv.FormatInt(int64(*request.Status), 10))
	}
	for idx, val := range request.TagValue {
		tags.WriteString(" ")
		tags.WriteString(request.Dictionary[request.TagName[idx]])
		tags.WriteString("=")
		tags.WriteString(request.Dictionary[val])
	}
	return tags.String()
}

func GetTimers(request *Request) []string {
	offset := 0
	timers := make([]string, len(request.TimerValue))
	for idx, val := range request.TimerValue {
		var timer bytes.Buffer
		var cputime float64 = 0.0
		if len(request.TimerUtime) == len(request.TimerValue) {
			cputime = float64(request.TimerUtime[idx] + request.TimerStime[idx])
		}

		timer.WriteString("Val: ")
		timer.WriteString(strconv.FormatFloat(float64(val), 'f', 4, 64))
		timer.WriteString(" Hit: ")
		timer.WriteString(strconv.FormatInt(int64(request.TimerHitCount[idx]), 10))
		timer.WriteString(" CPU: ")
		timer.WriteString(strconv.FormatFloat(cputime, 'f', 4, 64))
		timer.WriteString(" Tags: ")

		for k, key_idx := range request.TimerTagName[offset : offset+int(request.TimerTagCount[idx])] {
			val_idx := request.TimerTagValue[int(offset)+k]
			if val_idx >= uint32(len(request.Dictionary)) || key_idx >= uint32(len(request.Dictionary)) {
				continue
			}
			timer.WriteString(" ")
			timer.WriteString(request.Dictionary[key_idx])
			timer.WriteString("=")
			timer.WriteString(request.Dictionary[val_idx])
		}

		timers[idx] = timer.String()
		offset += int(request.TimerTagCount[idx])
	}
	return timers
}
