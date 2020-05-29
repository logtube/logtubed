package types

import (
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var ErrInvalidCompactEvent = errors.New("invalid compact event")

// CompactEvent compact version of event
type CompactEvent struct {
	Timestamp int64                  `json:"t"`           // the time when record produced
	Hostname  string                 `json:"h"`           // the server where record produced
	Env       string                 `json:"e"`           // environment where record produced, for example 'dev'
	Project   string                 `json:"p"`           // project name
	Topic     string                 `json:"o"`           // topic of log, for example 'access', 'err'
	Crid      string                 `json:"c"`           // correlation id
	Crsrc     string                 `json:"s"`           // correlation source
	Message   string                 `json:"m,omitempty"` // the actual log message body
	Keyword   string                 `json:"k"`           // comma separated keywords
	Extra     map[string]interface{} `json:"x,omitempty"` // extra structured data
}

func UnmarshalCompactEventJSON(buf []byte) (c CompactEvent, err error) {
	if err = json.Unmarshal(buf, &c); err != nil {
		return
	}
	trimStr(&c.Hostname)
	trimStr(&c.Env)
	trimStr(&c.Topic)
	trimStr(&c.Project)
	if len(c.Env)+len(c.Topic)+len(c.Project) == 0 {
		err = ErrInvalidCompactEvent
	}
	return
}

func (c CompactEvent) ToEvent() (e Event) {
	e.Timestamp = time.Unix(c.Timestamp/1000, int64(time.Millisecond/time.Nanosecond)*(c.Timestamp%1000))
	e.Hostname = c.Hostname
	e.Env = c.Env
	e.Project = c.Project
	e.Topic = c.Topic
	e.Crid = c.Crid
	e.Crsrc = c.Crsrc
	e.Message = c.Message
	e.Keyword = c.Keyword
	e.Extra = c.Extra
	return
}

func trimStr(str *string) {
	*str = strings.TrimSpace(*str)
}
