package main

import (
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	"time"
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

func (m *Request) Valid() bool {
	return len(m.TimerHitCount) == len(m.TimerValue) &&
		len(m.TimerValue) == len(m.TimerTagCount) &&
		len(m.TimerTagCount) == len(m.TimerUtime) &&
		len(m.TimerUtime) == len(m.TimerStime)
}

type Metric struct {
	Time  int64
	Name  string
	Tags  map[string]string
	Value float32
}

func (d *Metric) String() (str string) {
	// put sys.cpu.user 1356998400 42.5 host=webserver01 cpu=0
	str = fmt.Sprintf("%s %d %f", d.Name, d.Time, d.Value)
	for k, v := range d.Tags {
		str += fmt.Sprintf(" %v=%v", k, v)
	}
	str += fmt.Sprintf("\n")
	return str
}

func (d *Metric) GoString() (str string) {
	return fmt.Sprintf("%#v", d)
}

var DecodeTime time.Duration

func Decode(ts int64, data []byte) (metrics []*Metric, err error) {
	defer func(data []byte) {
		if r := recover(); r != nil {
			err = r.(error)
			// fmt.Printf("Recovered from '%v'\n", r)
			// fmt.Println("\n-----\n")
			// fmt.Printf("var data = %#v \n", data)
			// fmt.Println("\n-----\n")
		}
	}(data)

	start := time.Now()

	request := &Request{}
	proto.Unmarshal(data, request)

	if !request.Valid() {
		// fmt.Println("\n---invalid data---")
		// fmt.Printf("var data = %#v \n", data)
		// fmt.Println("-----\n")
		return nil, fmt.Errorf("request is invalid")
	}

	tags := map[string]string{}
	tags["host"] = *request.Hostname
	tags["server"] = *request.ServerName
	tags["script"] = *request.ScriptName
	if request.Status != nil {
		tags["status"] = fmt.Sprintf("%d", *request.Status)
	}

	for idx, val := range request.TagValue {
		tags[request.Dictionary[request.TagName[idx]]] = request.Dictionary[val]
	}

	offset := 0
	metric_idx := 0
	metrics_cnt := len(request.TimerValue)*3 + 3
	metrics = make([]*Metric, metrics_cnt)

	for idx, val := range request.TimerValue {
		timer_tags := map[string]string{}
		for k, v := range tags {
			timer_tags[k] = v
		}

		for k, key_idx := range request.TimerTagName[offset : offset+int(request.TimerTagCount[idx])] {
			val_idx := request.TimerTagValue[int(offset)+k]
			if val_idx >= uint32(len(request.Dictionary)) || key_idx >= uint32(len(request.Dictionary)) {
				continue
			}
			timer_tags[request.Dictionary[key_idx]] = request.Dictionary[val_idx]
		}

		metrics[metric_idx] = &Metric{
			Time:  ts,
			Name:  "timers.time",
			Value: val,
			Tags:  timer_tags,
		}
		metric_idx++
		metrics[metric_idx] = &Metric{
			Time:  ts,
			Name:  "timers.cnt",
			Value: float32(request.TimerHitCount[idx]),
			Tags:  timer_tags,
		}
		metric_idx++
		metrics[metric_idx] = &Metric{
			Time:  ts,
			Name:  "timers.cpu",
			Value: request.TimerUtime[idx] + request.TimerStime[idx],
			Tags:  timer_tags,
		}
		metric_idx++
		offset += int(request.TimerTagCount[idx])
	}

	metrics[metric_idx] = &Metric{
		Time:  ts,
		Name:  "php.time",
		Value: *request.RequestTime,
		Tags:  tags,
	}
	metric_idx++
	metrics[metric_idx] = &Metric{
		Time:  ts,
		Name:  "php.cpu",
		Value: *request.RuUtime + *request.RuStime,
		Tags:  tags,
	}
	metric_idx++
	metrics[metric_idx] = &Metric{
		Time:  ts,
		Name:  "php.cnt",
		Value: 1,
		Tags:  tags,
	}

	DecodeTime += time.Now().Sub(start)
	return metrics, nil
}
