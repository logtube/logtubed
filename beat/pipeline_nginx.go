package beat

import (
	"github.com/logtube/logtubed/pkg/compactkv"
	"github.com/logtube/logtubed/types"
	"github.com/rs/zerolog/log"
	"strings"
	"time"
)

const (
	// decode nginx $time_iso8601 '11/Mar/2020:19:03:53 +0800'
	ngxFormatISO8601 = `2006-01-02T15:04:05-07:00`
)

var (
	ngxTagNames = map[string]string{
		"bbs":  "body_bytes_sent",
		"hh":   "http_host",
		"hr":   "http_referer",
		"hua":  "http_user_agent",
		"hxff": "http_x_forwarded_for",
		"r":    "request",
		"ra":   "remote_addr",
		"rt":   "request_time",
		"s":    "status",
		"ua":   "upstream_addr",
		"urt":  "upstream_response_time",
	}
	ngxTagTypes = map[string]compactkv.ValueType{
		"bbs": compactkv.IntegerType,
		"rt":  compactkv.FloatType,
		"s":   compactkv.IntegerType,
		"urt": compactkv.FloatType,
	}
	ngxVarsUnderSecond = map[string]bool{
		"request_time":           true,
		"upstream_response_time": true,
	}
	ngxVarRequest = "request"
)

var (
	nginxCompactKV = compactkv.NewCompactKV()
)

func init() {
	for tag, name := range ngxTagNames {
		nginxCompactKV.Add(tag, name, ngxTagTypes[tag])
	}
}

type NginxPipelineOptions struct {
}

func NewNginxPipeline(opts NginxPipelineOptions) Pipeline {
	return &ngxPipeline{}
}

type ngxPipeline struct {
}

func (n *ngxPipeline) Name() string {
	return "nginx"
}

func (n *ngxPipeline) Match(b Event) bool {
	return b.Fileset.Module == "nginx" && b.Fileset.Name == "access"
}

func (n *ngxPipeline) Process(b Event, r *types.Event) (success bool) {
	r.Topic = "x-nginx-access"
	r.Hostname = b.Beat.Hostname
	r.Env = NONAME
	r.Project = NONAME
	r.Crid = "-"
	r.Extra = map[string]interface{}{
		"file": b.Source,
	}

	// search for [...]
	lb := strings.Index(b.Message, "[")
	rb := strings.Index(b.Message, "]")
	if lb < 0 || rb < 0 || lb > rb {
		log.Debug().Int("lb", lb).Int("rb", rb).Msg("nginx_pipeline: invalid bucket location")
		return
	}

	// decode $time_iso8601
	var err error
	if r.Timestamp, err = time.Parse(ngxFormatISO8601, b.Message[lb+1:rb]); err != nil {
		log.Debug().Err(err).Msg("nginx_pipeline: bad timestamp")
		return
	}

	// decode compact kv
	m := nginxCompactKV.Parse(b.Message[rb+1:])
	for k, v := range m {
		if ngxVarsUnderSecond[k] {
			// translate float seconds to integer milliseconds
			if vFloat, ok := v.(float64); ok {
				r.Extra[k] = int64(vFloat * 1000)
			}
		} else if k == ngxVarRequest {
			// decode $request
			if request, ok := v.(string); ok {
				splits := strings.SplitN(request, " ", 3)
				if len(splits) == 3 {
					r.Extra["method"] = splits[0]
					r.Extra["path"] = splits[1]
					r.Extra["protocol"] = splits[2]
				}
			}
		} else {
			r.Extra[k] = v
		}
	}

	success = true
	return
}
