package main

import (
	"github.com/logtube/sptp"
	"github.com/rs/zerolog/log"
	"net"
)

type SPTPInput struct {
	Options SPTPInputOptions

	conn *net.UDPConn
	recv sptp.Receiver
	done bool
}

func NewSPTPInput(options SPTPInputOptions) (input *SPTPInput, err error) {
	var addr *net.UDPAddr
	if addr, err = net.ResolveUDPAddr("udp", options.Bind); err != nil {
		return
	}
	input = &SPTPInput{Options: options}
	if input.conn, err = net.ListenUDP("udp", addr); err != nil {
		return
	}
	input.recv = sptp.NewReceiver(input.conn)
	return
}

func (s *SPTPInput) Close() error {
	s.done = true
	return s.conn.Close()
}

func (s *SPTPInput) Run(queue chan Event) error {
	var err error
	for {
		if s.done {
			break
		}

		var buf []byte

		if buf, err = s.recv.Receive(); err != nil {
			continue
		}
		var ce CompactEvent
		if ce, err = UnmarshalCompactEventJSON(buf); err != nil {
			continue
		}
		log.Debug().Str("input", "SPTP").Interface("event", ce).Msg("new event")

		queue <- ce.ToEvent()
	}
	return nil
}
