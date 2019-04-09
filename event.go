package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event a log event
type Event struct {
	Timestamp time.Time              `json:"timestamp"`         // the time when record produced
	Hostname  string                 `json:"hostname"`          // the server where record produced
	Env       string                 `json:"env"`               // environment where record produced, for example 'dev'
	Project   string                 `json:"project"`           // project name
	Topic     string                 `json:"topic"`             // topic of log, for example 'access', 'err'
	Crid      string                 `json:"crid"`              // correlation id
	Message   string                 `json:"message,omitempty"` // the actual log message body
	Keyword   string                 `json:"keyword"`           // comma separated keywords
	Extra     map[string]interface{} `json:"extra,omitempty"`   // extra structured data
}

func (r Event) Map() (out map[string]interface{}) {
	out = map[string]interface{}{}
	// assign extra with prefix
	for k, v := range r.Extra {
		out["x_"+k] = v
	}
	// assign fields manually
	out["timestamp"] = r.Timestamp.Format(time.RFC3339Nano)
	out["hostname"] = r.Hostname
	out["env"] = r.Env
	out["project"] = r.Project
	out["topic"] = r.Topic
	out["crid"] = r.Crid
	out["keyword"] = r.Keyword
	if len(r.Message) > 0 {
		out["message"] = r.Message
	}
	return
}

// Index index for record in ElasticSearch
func (r Event) Index() string {
	return fmt.Sprintf("%s-%s-%s-%04d-%02d-%02d", r.Topic, r.Env, r.Project, r.Timestamp.Year(), r.Timestamp.Month(), r.Timestamp.Day())
}

// ToOperation convert record to operation
func (r Event) ToOperation() (o Operation) {
	o.Index = r.Index()
	o.Body, _ = json.Marshal(r.Map())
	return
}
