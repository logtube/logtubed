package types

import (
	"errors"
	"github.com/rs/zerolog/log"
)

/*
  Filebeat 6.3.2 mysql - error

{
  '@timestamp': '2019-10-15T07:46:43.395Z',
  '@metadata': {
    beat: '',
    type: 'doc',
    version: '6.3.2',
    pipeline: 'filebeat-6.3.2-mysql-error-pipeline'
  },
  message: '2019-10-15T07:21:09.037619Z 0 [Note] Event Scheduler: Loaded 0 events',
  prospector: { type: 'log' },
  input: { type: 'log' },
  fileset: { name: 'error', module: 'mysql' },
  beat: { version: '6.3.2', name: 'xlog.kibana', hostname: 'xlog.kibana' },
  host: { name: 'xlog.kibana' },
  source: '/var/log/mysqld.log',
  offset: 20000
}

  Filebeat 6.3.2 mysql - slowlog

{
  '@timestamp': '2019-10-15T09:24:06.483Z',
  '@metadata': {
    beat: '',
    type: 'doc',
    version: '6.3.2',
    pipeline: 'filebeat-6.3.2-mysql-slowlog-pipeline'
  },
  message: '# User@Host: root[root] @ localhost []  Id:     2\n' +
    '# Query_time: 10.000248  Lock_time: 0.000000 Rows_sent: 1  Rows_examined: 0\n' +
    'SET timestamp=1571131440;\n' +
    'select sleep(10);',
  source: '/var/log/mysqld-slow.log',
  fileset: { module: 'mysql', name: 'slowlog' },
  prospector: { type: 'log' },
  input: { type: 'log' },
  beat: { name: 'xlog.kibana', hostname: 'xlog.kibana', version: '6.3.2' },
  host: { name: 'xlog.kibana' },
  offset: 216
}

*/

type BeatPipeline struct {
	Module  string
	Name    string
	Process func(b BeatEvent, r *Event) error
}

var (
	Pipelines = []BeatPipeline{
		// mysql - error
		{
			Module: "mysql",
			Name:   "error",
			Process: func(b BeatEvent, r *Event) (err error) {
				err = errors.New("unimplemented")
				return
			},
		},
		// mysql - slow log
		{
			Module: "mysql",
			Name:   "slowlog",
			Process: func(b BeatEvent, r *Event) (err error) {
				err = errors.New("unimplemented")
				return
			},
		},
	}
)

func RunPipelines(b BeatEvent, r *Event) (found bool, success bool) {
	for _, p := range Pipelines {
		if b.Fileset.Module == p.Module && b.Fileset.Name == p.Name {
			if err := p.Process(b, r); err != nil {
				log.Error().Err(err).Str("module", b.Fileset.Module).Str("name", b.Fileset.Name).Msg("failed to run pipeline")
				return true, false
			}
			return true, true
		}
	}
	if len(b.Fileset.Module) > 0 || len(b.Fileset.Name) > 0 {
		log.Debug().Str("module", b.Fileset.Module).Str("name", b.Fileset.Name).Msg("missing module support")
	}
	return false, false
}
