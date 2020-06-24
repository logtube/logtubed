package beat

import (
	"bytes"
	"encoding/json"
	"github.com/logtube/logtubed/types"
	"github.com/guoyk93/byteflow"
	"strings"
	"time"
)

/*

Pipeline Logtube:

V1 Message: [2018/09/10 17:24:22.120] CRID[945bea8e42de2796] this is a message
V2 Message: [2018-09-10 17:24:22.120 +0800] CRID[945bea8e42de2796] this is a message
v2.1 Message: [2018-09-10 17:24:22.120 +0800] [{"c":"xxxxxxx"}] this is a message

*/

const (
	LogtubeV2TimestampLayout = "2006-01-02 15:04:05.000 -0700"
)

type LogtubePipelineOptions struct {
	DefaultTimeOffset int
}

func NewLogtubePipeline(opts LogtubePipelineOptions) Pipeline {
	return &logtubePipeline{opts: opts}
}

type logtubePipeline struct {
	opts LogtubePipelineOptions
}

func (l *logtubePipeline) Name() string {
	return "logtube"
}

func (l *logtubePipeline) Match(b Event) bool {
	return true
}

func (l *logtubePipeline) Process(b Event, r *types.Event) (ok bool) {
	// assign hostname
	r.Hostname = b.Beat.Hostname
	// decode source field
	if ok = decodeLogtubeBeatSource(b.Source, r); !ok {
		return
	}
	// trim message
	b.Message = strings.TrimSpace(b.Message)
	// detect v2 message
	if isLogtubeV2Message(b.Message) {
		// decode v2 message field
		if ok = decodeLogtubeV2BeatMessage(b.Message, r); !ok {
			return
		}
	} else {
		// decode v1 message field
		var noOffset bool
		if noOffset, ok = decodeLogtubeV1Message(b.Message, strings.Contains(r.Topic, "_json_"), r); !ok {
			return
		}
		if !noOffset {
			r.Timestamp = r.Timestamp.Add(time.Hour * time.Duration(l.opts.DefaultTimeOffset))
		}
	}
	return
}

func isLogtubeV2Message(raw string) bool {
	if len(raw) < 31 {
		return false
	}
	raw = raw[0:31]
	if !strings.HasPrefix(raw, "[") {
		return false
	}
	if !strings.HasSuffix(raw, "]") {
		return false
	}
	raw = raw[5:26]
	if !strings.HasPrefix(raw, "-") {
		return false
	}
	return strings.HasSuffix(raw, "-") || strings.HasSuffix(raw, "+")
}

func decodeLogtubeV2BeatMessage(raw string, r *types.Event) (ok bool) {
	// check length
	if len(raw) < 32 {
		return
	}

	// decode timestamp
	var err error
	if r.Timestamp, err = time.Parse(LogtubeV2TimestampLayout, raw[1:30]); err != nil {
		return
	}

	// remaining
	buf := bytes.TrimSpace([]byte(raw[31:]))

	if bytes.HasPrefix(buf, []byte("{")) && bytes.HasSuffix(buf, []byte("}")) {
		var p PartialEvent
		if err = json.Unmarshal(buf, &p); err != nil {
			return
		}
		r.Crid = p.Crid
		r.Crsrc = p.Crsrc
		r.Message = p.Message
		r.Keyword = p.Keyword
		r.Extra = p.Extra
		ok = true
		return
	} else if bytes.HasPrefix(buf, []byte("[{")) {
		var p PartialEvent
		br := bytes.NewReader(buf)
		dec := json.NewDecoder(br)
		if _, err = dec.Token(); err != nil {
			return
		}
		if !dec.More() {
			return
		}
		if err = dec.Decode(&p); err != nil {
			return
		}
		if _, err = dec.Token(); err != nil {
			return
		}
		r.Crid = p.Crid
		r.Crsrc = p.Crsrc
		r.Keyword = p.Keyword
		r.Extra = p.Extra
		// buf is larger than any of the remaining bytes
		// construct r.Message with buf as buffer to reduce memory usage
		nj, _ := dec.Buffered().Read(buf) // rest of json decoder
		nm, _ := br.Read(buf[nj:])        // rest of the message
		r.Message = string(bytes.TrimSpace(buf[0 : nj+nm]))
		ok = true
		return
	} else {
		// extract CRID, KEYWORD
		if buf, _, ok = byteflow.Run(
			buf,
			byteflow.TrimOp{Left: true, Right: true},
			byteflow.MarkDecodeOp{Name: "CRID", Out: &r.Crid},
			byteflow.MarkDecodeOp{Name: "K", Out: &r.Keyword, Combine: true, Separator: ","},
			byteflow.MarkDecodeOp{Name: "KW", Out: &r.Keyword, Combine: true, Separator: ","},
			byteflow.MarkDecodeOp{Name: "KEYWORD", Out: &r.Keyword, Combine: true, Separator: ","},
			byteflow.TrimOp{Left: true, Right: true},
		); !ok {
			return
		}
		// assign the remaining message
		r.Message = string(buf)
	}

	return
}

func decodeLogtubeV1Message(raw string, isJSON bool, r *types.Event) (noOffset bool, ok bool) {
	var yyyy, MM, dd, hh, mm, ss, SSS int64
	buf := []byte(raw)
	if buf, _, ok = byteflow.Run(
		buf,
		byteflow.RuneOp{Remove: true, Allowed: []rune{'['}},
		byteflow.IntOp{Remove: true, Len: 4, Base: 10, Out: &yyyy},
		byteflow.RuneOp{Remove: true, Allowed: []rune{'-', '/'}},
		byteflow.IntOp{Remove: true, Len: 2, Base: 10, Out: &MM},
		byteflow.RuneOp{Remove: true, Allowed: []rune{'-', '/'}},
		byteflow.IntOp{Remove: true, Len: 2, Base: 10, Out: &dd},
		byteflow.RuneOp{Remove: true, Allowed: []rune{' ', '\t'}},
		byteflow.IntOp{Remove: true, Len: 2, Base: 10, Out: &hh},
		byteflow.RuneOp{Remove: true, Allowed: []rune{':'}},
		byteflow.IntOp{Remove: true, Len: 2, Base: 10, Out: &mm},
		byteflow.RuneOp{Remove: true, Allowed: []rune{':'}},
		byteflow.IntOp{Remove: true, Len: 2, Base: 10, Out: &ss},
		byteflow.RuneOp{Remove: true, Allowed: []rune{'.'}},
		byteflow.IntOp{Remove: true, Len: 3, Base: 10, Out: &SSS},
		byteflow.RuneOp{Remove: true, Allowed: []rune{']'}},
	); !ok {
		return
	}
	// extract the timestamp
	r.Timestamp = time.Date(int(yyyy), time.Month(MM), int(dd), int(hh), int(mm), int(ss), int(int64(time.Millisecond)*SSS), time.UTC)
	// extract extra or CRID/K
	if isJSON {
		if buf, _, ok = byteflow.Run(
			buf,
			byteflow.TrimOp{Left: true, Right: true},
			byteflow.JSONDecodeOp{Remove: true, Out: &r.Extra},
		); !ok {
			return
		}
		// topic must exist
		if !decodeLogtubeExtraStr(r.Extra, "topic", &r.Topic) {
			ok = false
			return
		}
		// optional extra 'project', 'crid'
		decodeLogtubeExtraStr(r.Extra, "project", &r.Project)
		decodeLogtubeExtraStr(r.Extra, "crid", &r.Crid)
		// optional extract 'timestamp'
		if decodeLogtubeExtraTime(r.Extra, "timestamp", &r.Timestamp) {
			noOffset = true
		}
		// clear the message
		r.Message = ""
	} else {
		if buf, _, ok = byteflow.Run(
			buf,
			byteflow.TrimOp{Left: true, Right: true},
			byteflow.MarkDecodeOp{Name: "CRID", Out: &r.Crid},
			byteflow.MarkDecodeOp{Name: "K", Out: &r.Keyword, Combine: true, Separator: ","},
			byteflow.MarkDecodeOp{Name: "KW", Out: &r.Keyword, Combine: true, Separator: ","},
			byteflow.MarkDecodeOp{Name: "KEYWORD", Out: &r.Keyword, Combine: true, Separator: ","},
			byteflow.TrimOp{Left: true, Right: true},
		); !ok {
			return
		}
		// assign the remaining message
		r.Message = string(buf)
	}
	return
}

func decodeLogtubeBeatSource(raw string, r *types.Event) bool {
	var cs []string
	// split source with / separator, fxxk windows
	cs = strings.Split(strings.TrimSpace(raw), "/")
	if len(cs) < 1 {
		return false
	}
	// support for dot separated filename
	filename := cs[len(cs)-1]
	fnsplits := strings.Split(filename, ".")
	if len(fnsplits) > 3 {
		r.Env, r.Topic, r.Project = fnsplits[0], fnsplits[1], fnsplits[2]
		return true
	}
	// check length
	if len(cs) < 3 {
		return false
	}
	// assign fields
	r.Env, r.Topic, r.Project = cs[len(cs)-3], cs[len(cs)-2], cs[len(cs)-1]
	// sanitize dot separated filename
	var ss []string
	if ss = strings.Split(r.Project, "."); len(ss) > 0 {
		r.Project = ss[0]
	}
	return true
}

func decodeLogtubeExtraStr(m map[string]interface{}, key string, out *string) bool {
	if m == nil || out == nil {
		return false
	}
	if val, ok := m[key].(string); ok {
		val = strings.TrimSpace(val)
		delete(m, key) // always delete
		if len(val) > 0 {
			*out = val // update if not empty
			return true
		}
	}
	return false
}

func decodeLogtubeExtraTime(m map[string]interface{}, key string, out *time.Time) bool {
	if m == nil || out == nil {
		return false
	}
	var tsStr string
	if decodeLogtubeExtraStr(m, key, &tsStr) {
		if t, err := time.Parse(time.RFC3339, tsStr); err != nil {
			return false
		} else {
			*out = t // update if success
			return true
		}
	}
	return false
}
