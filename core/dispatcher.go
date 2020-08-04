package core

import (
	"errors"
	"github.com/logtube/logtubed/types"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
	"strings"
)

type DispatcherOptions struct {
	TopicIgnores         []string
	TopicRequireKeywords []string
	KeywordIgnores       []string
	Priors               []string
	EnvMappings          map[string]string
	TopicMappings        map[string]string

	Hostname string

	Next    types.EventConsumer
	NextStd types.OpConsumer
	NextPri types.OpConsumer
}

type dispatcher struct {
	Hostname string

	tIgn map[string]bool
	tKey map[string]bool
	tPri map[string]bool
	kIgn map[string]bool
	mE   map[string]string
	mT   map[string]string

	next    types.EventConsumer
	nextStd types.OpConsumer
	nextPri types.OpConsumer
}

func NewDispatcher(opts DispatcherOptions) (types.EventConsumer, error) {
	if len(opts.Hostname) == 0 {
		opts.Hostname = "localhost"
	}
	if opts.NextStd == nil && opts.NextPri == nil && opts.Next == nil {
		return nil, errors.New("non of NextStd, NextPri, Next is specified")
	}

	log.Info().Interface("opts", opts).Msg("dispatcher created")
	d := &dispatcher{
		Hostname: opts.Hostname,
		tIgn:     make(map[string]bool),
		tKey:     make(map[string]bool),
		kIgn:     make(map[string]bool),
		tPri:     make(map[string]bool),
		mE:       make(map[string]string),
		mT:       make(map[string]string),
		nextStd:  opts.NextStd,
		nextPri:  opts.NextPri,
		next:     opts.Next,
	}
	for _, t := range opts.TopicIgnores {
		d.tIgn[t] = true
	}
	for _, t := range opts.TopicRequireKeywords {
		d.tKey[t] = true
	}
	for _, t := range opts.Priors {
		d.tPri[t] = true
	}
	for _, k := range opts.KeywordIgnores {
		d.kIgn[k] = true
	}
	for k, v := range opts.EnvMappings {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k != "" && v != "" {
			d.mE[k] = v
		}
	}
	for k, v := range opts.TopicMappings {
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if k != "" && v != "" {
			d.mT[k] = v
		}
	}
	return d, nil
}

func (d *dispatcher) shouldDropEvent(e types.Event) bool {
	// check ignores
	if d.tIgn[e.Topic] {
		return true
	}
	// check keyword required
	if d.tKey[e.Topic] && len(e.Keyword) == 0 {
		return true
	}
	// check keyword ignored
	if d.kIgn[e.Keyword] {
		return true
	}
	// check HEAD /
	if e.Topic == "x-access" && e.Extra != nil {
		path, _ := e.Extra["path"].(string)
		method, _ := e.Extra["method"].(string)
		if path == "/" && strings.ToLower(method) == "head" {
			return true
		}
	}
	return false
}

func (d *dispatcher) ConsumeEvent(e types.Event) error {
	// assign via
	e.Via = d.Hostname
	// check drop
	if d.shouldDropEvent(e) {
		return nil
	}
	// rewrite env / topic
	if ne, ok := d.mE[e.Env]; ok {
		e.Env = ne
	}
	if nt, ok := d.mT[e.Topic]; ok {
		e.Topic = nt
	}
	eg := common.NewErrorGroup()
	if d.next != nil {
		// delivery to Next, i.e. LocalOutput, if set
		eg.Add(d.next.ConsumeEvent(e))
	}
	if d.nextStd != nil || d.nextPri != nil {
		op := e.ToOp()
		if d.tPri[e.Topic] && d.nextPri != nil {
			// delivery to NextPri, i.e. Queue Pri, if set
			eg.Add(d.nextPri.ConsumeOp(op))
		} else if d.nextStd != nil {
			// delivery to NextStd, i.e. Queue Std, if set
			eg.Add(d.nextStd.ConsumeOp(op))
		}
	}
	return eg.Err()
}
