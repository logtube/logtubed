package core

import (
	"errors"
	"github.com/logtube/logtubed/types"
	"github.com/rs/zerolog/log"
	"go.guoyk.net/common"
)

type DispatcherOptions struct {
	Ignores  []string
	Keywords []string
	Priors   []string

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
		tPri:     make(map[string]bool),
		nextStd:  opts.NextStd,
		nextPri:  opts.NextPri,
		next:     opts.Next,
	}
	for _, t := range opts.Ignores {
		d.tIgn[t] = true
	}
	for _, t := range opts.Keywords {
		d.tKey[t] = true
	}
	for _, t := range opts.Priors {
		d.tPri[t] = true
	}
	return d, nil
}

func (d *dispatcher) ConsumeEvent(e types.Event) error {
	// assign via
	e.Via = d.Hostname
	// check ignores
	if d.tIgn[e.Topic] {
		return nil
	}
	// check keyword required
	if d.tKey[e.Topic] && len(e.Keyword) == 0 {
		return nil
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
