package beat

// Event a single event in redis sent by filebeat
type Event struct {
	Beat struct {
		Hostname string `json:"hostname"`
	} `json:"beat"`                 // contains hostname
	Message string `json:"message"` // contains timestamp, crid
	Source  string `json:"source"`  // contains env, topic, project
	Fileset struct {
		Module string `json:"module"`
		Name   string `json:"name"`
	} `json:"fileset"` // contains module, name
}

type PartialEvent struct {
	Crid    string                 `json:"c"`
	Crsrc   string                 `json:"s"`
	Message string                 `json:"m"`
	Keyword string                 `json:"k"`
	Extra   map[string]interface{} `json:"x"`
}
