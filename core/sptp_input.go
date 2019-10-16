package core

import (
	"context"
	"errors"
	"github.com/logtube/logtubed/types"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"go.guoyk.net/sptp"
	"net"
)

type SPTPInputOptions struct {
	Bind string
	Next types.EventConsumer
}

type SPTPInput interface {
	common.Runnable
}

type sptpInput struct {
	next types.EventConsumer
	addr *net.UDPAddr
}

func NewSPTPInput(opts SPTPInputOptions) (SPTPInput, error) {
	if len(opts.Bind) == 0 {
		opts.Bind = "0.0.0.0:9921"
	}
	if opts.Next == nil {
		return nil, errors.New("SPTPInput: Next not set")
	}
	var addr *net.UDPAddr
	var err error
	if addr, err = net.ResolveUDPAddr("udp", opts.Bind); err != nil {
		return nil, err
	}
	log.Info().Str("input", "sptp").Interface("opts", opts).Msg("input created")
	input := &sptpInput{
		addr: addr,
		next: opts.Next,
	}
	return input, nil
}

func (s *sptpInput) Run(ctx context.Context) error {
	log.Info().Str("input", "SPTP").Msg("started")
	defer log.Info().Str("input", "SPTP").Msg("stopped")

	var closing bool
	var conn *net.UDPConn
	var err error
	// UDP server
	if conn, err = net.ListenUDP("udp", s.addr); err != nil {
		log.Error().Err(err).Str("input", "SPTP").Msg("failed to bind UDP socket")
		return err
	}
	// SPTP receiver
	recv := sptp.NewReceiver(conn)
	// wait context cancellation
	go func() {
		<-ctx.Done()
		closing = true
		_ = conn.Close()
	}()
	// the main loop
	for {
		var buf []byte
		if buf, err = recv.Receive(); err != nil {
			if closing {
				return nil
			}
			log.Error().Err(err).Str("input", "SPTP").Msg("failed to read SPTP packet")
			continue
		}

		if buf == nil {
			continue
		}

		var ce types.CompactEvent
		if ce, err = types.UnmarshalCompactEventJSON(buf); err != nil {
			continue
		}
		log.Debug().Str("input", "SPTP").Interface("event", ce).Msg("new event")

		if err = s.next.ConsumeEvent(ce.ToEvent()); err != nil {
			log.Error().Err(err).Str("input", "SPTP").Msg("failed to delivery event")
		}
	}
}
