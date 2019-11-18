package beat

import (
	"github.com/logtube/logtubed/types"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	NONAME = "noname"
)

/*
  Pipeline MySQL Error:

  2019-10-15T07:21:09.025737Z 0 [Warning] CA certificate ca.pem is self signed.
  2019-03-05 11:08:27 17054 [Note] /usr/local/mysql/bin/mysqld: ready for connections.
*/

type MySQLFormat struct {
	TimestampLayout string
	Format          *regexp.Regexp
}

var mySQLFormats = []MySQLFormat{
	{
		TimestampLayout: "2006-01-02T15:04:05.000000Z07:00",
		Format:          regexp.MustCompile(`^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6}.+)\s+(?P<thread_id>\d+)\s+\[(?P<level>\w+)]\s+(?P<message>.+)`),
	},
	{
		TimestampLayout: "2006-01-02 15:04:05",
		Format:          regexp.MustCompile(`^(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(?P<thread_id>\d+)\s+\[(?P<level>\w+)]\s+(?P<message>.+)`),
	},
}

type MySQLPipelineOptions struct {
	ErrorIgnoreLevels []string
}

func NewMySQLPipeline(opts MySQLPipelineOptions) Pipeline {
	return &mySQLErrorPipeline{opts: opts}
}

type mySQLErrorPipeline struct {
	opts MySQLPipelineOptions
}

func (m *mySQLErrorPipeline) Name() string {
	return "mysql"
}

func (m *mySQLErrorPipeline) Match(b Event) bool {
	return b.Fileset.Module == "mysql"
}

func (m *mySQLErrorPipeline) Process(b Event, r *types.Event) bool {
	r.Hostname = b.Beat.Hostname
	r.Env = NONAME
	r.Project = NONAME
	r.Crid = "-"
	r.Extra = map[string]interface{}{
		"file": b.Source,
	}
	if b.Fileset.Name == "error" {
		return m.decodeMySQLError(b, r)
	} else {
		return false
	}
}

func (m *mySQLErrorPipeline) decodeMySQLError(b Event, r *types.Event) bool {
	r.Topic = "x-mysql-error"
	b.Message = strings.TrimSpace(b.Message)
	for _, f := range mySQLFormats {
		subs := f.Format.FindStringSubmatch(b.Message)
		if len(subs) == 0 {
			continue
		}
		var err error
		if r.Timestamp, err = time.Parse(f.TimestampLayout, subs[1]); err != nil {
			continue
		}
		level := strings.TrimSpace(strings.ToLower(subs[3]))
		for _, l := range m.opts.ErrorIgnoreLevels {
			if l == level {
				return false
			}
		}
		r.Extra["thread_id"], _ = strconv.Atoi(subs[2])
		r.Extra["level"] = level
		r.Message = subs[4]
		return true
	}
	return false
}
