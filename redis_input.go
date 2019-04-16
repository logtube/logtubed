package main

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"github.com/yankeguo/redcon"
	"strings"
	"sync"
	"sync/atomic"
)

type RedisInput struct {
	Options RedisInputOptions

	server *redcon.Server

	connsCount    int64
	connsSum      map[string]int
	connsSumMutex sync.Locker
}

func NewRedisInput(options RedisInputOptions) (o *RedisInput, err error) {
	o = &RedisInput{
		Options:       options,
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

func (r *RedisInput) consumeRawEvent(raw []byte, queue chan Event) {
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
	if record, ok := event.ToEvent(r.Options.TimeOffset); ok {
		log.Debug().Str("input", "redis").Interface("event", record).Msg("new event")
		queue <- record
	} else {
		log.Debug().Str("event", string(raw)).Msg("failed to convert record")
	}
}

func (r *RedisInput) handle(conn redcon.Conn, cmd redcon.Command, queue chan Event) {
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
		if r.Options.Multi {
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
		conn.WriteInt64(int64(len(queue)))
	case "llen":
		conn.WriteInt64(int64(len(queue)))
	}
}

func (r *RedisInput) Close() error {
	if r.server != nil {
		return r.server.Close()
	}
	return nil
}

func (r *RedisInput) Run(queue chan Event) (err error) {
	r.server = redcon.NewServer(
		r.Options.Bind,
		func(conn redcon.Conn, cmd redcon.Command) {
			r.handle(conn, cmd, queue)
		},
		func(conn redcon.Conn) bool {
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
		},
		func(conn redcon.Conn, err error) {
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
		},
	)

	setup := make(chan error, 1)
	_ = r.server.ListenServeAndSignal(setup)
	err = <-setup
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
