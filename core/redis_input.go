package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/logtube/logtubed/beat"
	"github.com/logtube/logtubed/types"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"go.guoyk.net/redcon"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	SuffixCompactEvent = []byte(".compact")
)

type RedisInputOptions struct {
	Bind                   string
	Multi                  bool
	LogtubeTimeOffset      int
	MySQLErrorIgnoreLevels []string
	Next                   types.EventConsumer
}

type RedisInput interface {
	common.Runnable
	SetBlocked(blocked bool)
}

type redisInput struct {
	optBind  string
	optMulti bool

	connsCount    int64
	connsSum      map[string]int
	connsSumMutex sync.Locker

	pipelines []beat.Pipeline

	next types.EventConsumer

	blocked bool
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
		optBind:  opts.Bind,
		optMulti: opts.Multi,

		connsSum:      map[string]int{},
		connsSumMutex: &sync.Mutex{},

		pipelines: []beat.Pipeline{
			beat.NewMySQLPipeline(beat.MySQLPipelineOptions{
				ErrorIgnoreLevels: opts.MySQLErrorIgnoreLevels,
			}),
			beat.NewNginxPipeline(beat.NginxPipelineOptions{}),
			beat.NewLogtubePipeline(beat.LogtubePipelineOptions{
				DefaultTimeOffset: opts.LogtubeTimeOffset,
			}),
		},

		next: opts.Next,
	}
	return o, nil
}

func (r *redisInput) SetBlocked(blocked bool) {
	r.blocked = blocked
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

func (r *redisInput) runPipelines(b beat.Event, e *types.Event) (ok bool) {
	for _, p := range r.pipelines {
		if p.Match(b) {
			log.Debug().Str("input", "redis").Str("pipeline", p.Name()).Msg("pipeline matched")
			return p.Process(b, e)
		}
	}
	log.Debug().Str("input", "redis").Msg("no pipeline matched")
	return
}

func (r *redisInput) consumeCompactEvent(raw []byte) {
	// ignore event > 1mb
	if len(raw) > 1000000 {
		return
	}
	// warn event > 500k
	if len(raw) > 500000 {
		log.Warn().Int("raw-length", len(raw)).Msg("raw message larger than 500k")
	}
	log.Debug().Int("raw-length", len(raw)).Msg("raw message")
	var ce types.CompactEvent
	var err error
	if ce, err = types.UnmarshalCompactEventJSON(raw); err != nil {
		log.Debug().Err(err).Str("event", string(raw)).Msg("failed to unmarshal compact event")
		return
	}
	e := ce.ToEvent()
	e.RawSize = len(raw)
	log.Debug().Str("input", "redis").Interface("event", e).Msg("new event")
	if err = r.next.ConsumeEvent(e); err != nil {
		log.Error().Err(err).Str("input", "redis").Msg("failed to delivery event to next")
	}
}

func (r *redisInput) consumeBeatEvent(raw []byte) {
	// ignore event > 1mb
	if len(raw) > 1000000 {
		return
	}
	// warn event > 500k
	if len(raw) > 500000 {
		log.Warn().Int("raw-length", len(raw)).Msg("raw message larger than 500k")
	}
	log.Debug().Int("raw-length", len(raw)).Msg("raw message")
	// unmarshal beat event
	var be beat.Event
	if err := json.Unmarshal(raw, &be); err != nil {
		log.Debug().Err(err).Str("event", string(raw)).Msg("failed to unmarshal beat event")
		return
	}
	// convert to event
	var e types.Event
	e.RawSize = len(raw)
	if ok := r.runPipelines(be, &e); ok {
		log.Debug().Str("input", "redis").Interface("event", e).Msg("new event")
		if err := r.next.ConsumeEvent(e); err != nil {
			log.Error().Err(err).Str("input", "redis").Msg("failed to delivery event to next")
		}
	} else {
		log.Debug().Str("event", string(raw)).Msg("pipeline not success")
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
		// refuse on blocked
		if r.blocked {
			conn.WriteError("ERR blocked")
			return
		}
		// at least 3 arguments, RPUSH xlog "{....}"
		if len(cmd.Args) < 3 {
			conn.WriteError("ERR bad command '" + command + "'")
			return
		}
		// retrieve all events
		if bytes.HasSuffix(cmd.Args[1], SuffixCompactEvent) {
			for _, raw := range cmd.Args[2:] {
				r.consumeCompactEvent(raw)
			}
		} else {
			for _, raw := range cmd.Args[2:] {
				r.consumeBeatEvent(raw)
			}
		}
		conn.WriteInt64(0)
	case "llen":
		conn.WriteInt64(0)
	}
}

func (r *redisInput) handleConnect(conn redcon.Conn) bool {
	if r.blocked {
		log.Error().Str("reason", "blocked").Str("addr", conn.RemoteAddr()).Msg("connection refused")
		time.Sleep(time.Second)
		return false
	}
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
