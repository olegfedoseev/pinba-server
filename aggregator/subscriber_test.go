package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"github.com/stretchr/testify/assert"
	"testing"
	//	"time"
)

func TestSubscriber(t *testing.T) {
	pub, _ := zmq.NewSocket(zmq.PUB)
	defer pub.Close()
	pub.Bind("inproc://test")

	sub := receive("inproc://test", []string{"test"})
	_, err := pub.SendMessage("test", "test message")
	if err != nil {
		fmt.Println(err)
	}

	msg := <-sub

	assert.Equal(t, "test", msg[0])
	assert.Equal(t, "test message", msg[1])
}
