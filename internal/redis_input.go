package internal

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/logtube/logtubed/internal/runner"
	"github.com/logtube/logtubed/types"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/redcon"
	"strings"
	"sync"
	"sync/atomic"
)

type RedisInputOptions struct {
	Bind       string
	Multi      bool
	TimeOffset int
	Next       types.EventConsumer
}

type RedisInput interface {
	runner.Runnable
}

type redisInput struct {
	optBind       string
	optMulti      bool
	optTimeOffset int

	connsCount    int64
	connsSum      map[string]int
	connsSumMutex sync.Locker

	next types.EventConsumer
}

func NewRedisInput(opts RedisInputOptions) (RedisInput, error) {
	if len(opts.Bind) == 0 {
		opts.Bind = "0.0.0.0:6379"
	}
	if opts.Next == nil {
		return nil, errors.New("RedisInput: Next is not set")
	}
	log.Info().Str("input", "redis").Interface("opts", opts).Msg("input created")
	o := &redisInput{
		optBind:       opts.Bind,
		optMulti:      opts.Multi,
		optTimeOffset: opts.TimeOffset,

		connsSum:      map[string]int{},
		connsSumMutex: &sync.Mutex{},

		next: opts.Next,
	}
	return o, nil
}

func (r *redisInput) increaseConnsCount() int64 {
	return atomic.AddInt64(&r.connsCount, 1)
}

func (r *redisInput) decreaseConnsCount() int64 {
	return atomic.AddInt64(&r.connsCount, -1)
}

func (r *redisInput) increaseConnsSum(addr string) int {
	r.connsSumMutex.Lock()
	defer r.connsSumMutex.Unlock()
	i := extractIP(addr)
	r.connsSum[i] = r.connsSum[i] + 1
	return r.connsSum[i]
}

func (r *redisInput) decreaseConnsSum(addr string) int {
	r.connsSumMutex.Lock()
	defer r.connsSumMutex.Unlock()
	i := extractIP(addr)
	r.connsSum[i] = r.connsSum[i] - 1
	return r.connsSum[i]
}

func (r *redisInput) consumeRawEvent(raw []byte) {
	// ignore event > 1mb
	if len(raw) > 1000000 {
		return
	}
	// warn event > 500k
	if len(raw) > 500000 {
		log.Warn().Int("raw-length", len(raw)).Msg("raw message larger than 500k")
	}
	log.Debug().Int("raw-length", len(raw)).Msg("raw message")
	// unmarshal event
	var event types.BeatEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		log.Debug().Err(err).Str("event", string(raw)).Msg("failed to unmarshal event")
		return
	}
	// convert to record
	if record, ok := event.ToEvent(r.optTimeOffset); ok {
		log.Debug().Str("input", "redis").Interface("event", record).Msg("new event")
		if err := r.next.ConsumeEvent(record); err != nil {
			log.Error().Err(err).Str("input", "redis").Msg("failed to delivery event to next")
		}
	} else {
		log.Debug().Str("event", string(raw)).Msg("failed to convert record")
	}
}

func (r *redisInput) handleCommand(conn redcon.Conn, cmd redcon.Command) {
	// empty arguments, not possible
	if len(cmd.Args) == 0 {
		conn.WriteError("ERR bad command")
		return
	}
	// extract command
	command := strings.ToLower(string(cmd.Args[0]))
	log.Debug().Str("addr", conn.RemoteAddr()).Str("cmd", command).Int("args", len(cmd.Args)-1).Msg("new command")
	// handle command
	switch command {
	default:
		log.Error().Str("command", command).Msg("unknown message")
		conn.WriteError("ERR unknown command '" + command + "'")
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		_ = conn.Close()
	case "info":
		if r.optMulti {
			// declare as redis 2.4+, supports multiple values in RPUSH/LPUSH
			conn.WriteString("redis_version:2.4\r\n")
		} else {
			// declare as redis 2.4-, not support multiple values in RPUSH/LPUSH
			conn.WriteString("redis_version:2.3\r\n")
		}
	case "rpush", "lpush":
		// at least 3 arguments, RPUSH xlog "{....}"
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR bad command '" + command + "'")
			return
		}
		// retrieve all events
		for _, raw := range cmd.Args[2:] {
			r.consumeRawEvent(raw)
		}
		conn.WriteInt64(0)
	case "llen":
		conn.WriteInt64(0)
	}
}

func (r *redisInput) handleConnect(conn redcon.Conn) bool {
	log.Info().Int64(
		"conns",
		r.increaseConnsCount(),
	).Int(
		"conns-dup",
		r.increaseConnsSum(conn.RemoteAddr()),
	).Str(
		"addr",
		conn.RemoteAddr(),
	).Msg("connection established")
	return true
}

func (r *redisInput) handleDisconnect(conn redcon.Conn, err error) {
	log.Info().Err(err).Int64(
		"conns",
		r.decreaseConnsCount(),
	).Int(
		"conns-dup",
		r.decreaseConnsSum(conn.RemoteAddr()),
	).Str(
		"addr",
		conn.RemoteAddr(),
	).Msg("connection closed")
}

func (r *redisInput) Run(ctx context.Context) error {
	log.Info().Str("input", "redis").Msg("started")
	defer log.Info().Str("input", "redis").Msg("stopped")

	init := make(chan error, 1)
	done := make(chan error, 1)

	// create the server
	s := redcon.NewServer(r.optBind, r.handleCommand, r.handleConnect, r.handleDisconnect)

	// start the server
	go func() {
		done <- s.ListenServeAndSignal(init)
	}()
	// wait server initialization
	if err := <-init; err != nil {
		log.Error().Err(err).Str("input", "redis").Msg("failed to initialize redis input")
		return err
	}
	// wait context cancellation or server exit
	select {
	case <-ctx.Done():
		return s.Close()
	case err := <-done:
		return err
	}
}

func extractIP(addr string) string {
	c := strings.Split(addr, ":")
	if len(c) < 2 {
		return "UNKNOWN"
	} else if len(c) == 2 {
		return c[0]
	} else {
		return strings.Join(c[0:len(c)-1], ":")
	}
}
