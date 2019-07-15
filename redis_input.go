package main

import (
	"context"
	"encoding/json"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/redcon"
	"strings"
	"sync"
	"sync/atomic"
)

type RedisInput struct {
	optBind       string
	optMulti      bool
	optTimeOffset int

	connsCount    int64
	connsSum      map[string]int
	connsSumMutex sync.Locker
}

func NewRedisInput(options RedisInputOptions) (o *RedisInput, err error) {
	o = &RedisInput{
		optBind:       options.Bind,
		optMulti:      options.Multi,
		optTimeOffset: options.TimeOffset,

		connsSum:      map[string]int{},
		connsSumMutex: &sync.Mutex{},
	}
	return
}

func (r *RedisInput) increaseConnsCount() int64 {
	return atomic.AddInt64(&r.connsCount, 1)
}

func (r *RedisInput) decreaseConnsCount() int64 {
	return atomic.AddInt64(&r.connsCount, -1)
}

func (r *RedisInput) increaseConnsSum(addr string) int {
	r.connsSumMutex.Lock()
	defer r.connsSumMutex.Unlock()
	i := extractIP(addr)
	r.connsSum[i] = r.connsSum[i] + 1
	return r.connsSum[i]
}

func (r *RedisInput) decreaseConnsSum(addr string) int {
	r.connsSumMutex.Lock()
	defer r.connsSumMutex.Unlock()
	i := extractIP(addr)
	r.connsSum[i] = r.connsSum[i] - 1
	return r.connsSum[i]
}

func (r *RedisInput) consumeRawEvent(raw []byte, queue Queue) {
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
	var event BeatEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		log.Debug().Err(err).Str("event", string(raw)).Msg("failed to unmarshal event")
		return
	}
	// convert to record
	if record, ok := event.ToEvent(r.optTimeOffset); ok {
		log.Debug().Str("input", "redis").Interface("event", record).Msg("new event")
		_ = queue.PutEvent(record)
	} else {
		log.Debug().Str("event", string(raw)).Msg("failed to convert record")
	}
}

func (r *RedisInput) handle(conn redcon.Conn, cmd redcon.Command, queue Queue) {
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
			r.consumeRawEvent(raw, queue)
		}
		conn.WriteInt64(queue.Depth())
	case "llen":
		conn.WriteInt64(queue.Depth())
	}
}

func (r *RedisInput) handleCommand(queue Queue) func(conn redcon.Conn, cmd redcon.Command) {
	return func(conn redcon.Conn, cmd redcon.Command) {
		r.handle(conn, cmd, queue)
	}
}

func (r *RedisInput) handleConnect(conn redcon.Conn) bool {
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

func (r *RedisInput) handleDisconnect(conn redcon.Conn, err error) {
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

func (r *RedisInput) Run(ctx context.Context, cancel context.CancelFunc, queue Queue) (err error) {
	s := redcon.NewServer(r.optBind, r.handleCommand(queue), r.handleConnect, r.handleDisconnect)
	setup := make(chan error, 1)
	go func() {
		_ = s.ListenServeAndSignal(setup)
	}()
	if err = <-setup; err != nil {
		log.Error().Err(err).Str("input", "redis").Msg("failed to initialize redis input")
		cancel()
	}
	<-ctx.Done()
	_ = s.Close()
	return
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
