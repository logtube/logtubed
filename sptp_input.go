package main

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/sptp"
	"net"
)

type SPTPInput struct {
	optBind string

	addr *net.UDPAddr
}

func NewSPTPInput(options SPTPInputOptions) (input *SPTPInput, err error) {
	var addr *net.UDPAddr
	if addr, err = net.ResolveUDPAddr("udp", options.Bind); err != nil {
		return
	}
	input = &SPTPInput{
		optBind: options.Bind,
		addr:    addr,
	}
	return
}

func (s *SPTPInput) Run(ctx context.Context, cancel context.CancelFunc, output Queue) (err error) {
	var closing bool
	var conn *net.UDPConn
	if conn, err = net.ListenUDP("udp", s.addr); err != nil {
		log.Error().Err(err).Str("input", "SPTP").Msg("failed to bind UDP socket")
		cancel()
		return
	}
	go func() {
		<-ctx.Done()
		closing = true
		_ = conn.Close()
	}()
	recv := sptp.NewReceiver(conn)
	for {
		var buf []byte
		if buf, err = recv.Receive(); err != nil {
			if closing {
				err = nil
				break
			}
			log.Error().Err(err).Str("input", "SPTP").Msg("failed to read SPTP packet")
			continue
		}

		if buf == nil {
			continue
		}

		var ce CompactEvent
		if ce, err = UnmarshalCompactEventJSON(buf); err != nil {
			continue
		}
		log.Debug().Str("input", "SPTP").Interface("event", ce).Msg("new event")

		_ = output.PutEvent(ce.ToEvent())
	}
	return
}
