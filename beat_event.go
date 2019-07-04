package main

import (
	"bytes"
	"encoding/json"
	"go.guoyk.net/byteflow"
	"strings"
	"time"
)

/*

Examples:

V1 Message: [2018/09/10 17:24:22.120] CRID[945bea8e42de2796] this is a message
V2 Message: [2018-09-10 17:24:22.120 +0800] CRID[945bea8e42de2796] this is a message

*/

const (
	v2TimestampLayout = "2006-01-02 15:04:05.000 -0700"
)

// BeatEventBeat beat field in BeatEvent
type BeatEventBeat struct {
	Hostname string `json:"hostname"`
}

// BeatEvent a single event in redis sent by filebeat
type BeatEvent struct {
	Beat    BeatEventBeat `json:"beat"`    // contains hostname
	Message string        `json:"message"` // contains timestamp, crid
	Source  string        `json:"source"`  // contains env, topic, project
}

// ToEvent implements RecordConvertible
func (b BeatEvent) ToEvent(defaultTimeOffset int) (r Event, ok bool) {
	// assign hostname
	r.Hostname = b.Beat.Hostname
	// decode source field
	if ok = decodeBeatSource(b.Source, &r); !ok {
		return
	}
	// trim message
	b.Message = strings.TrimSpace(b.Message)
	// detect v2 message
	if isV2Message(b.Message) {
		// decode v2 message field
		if ok = decodeV2BeatMessage(b.Message, &r); !ok {
			return
		}
	} else {
		// decode v1 message field
		var noOffset bool
		if noOffset, ok = decodeV1BeatMessage(b.Message, strings.Contains(r.Topic, "_json_"), &r); !ok {
			return
		}
		if !noOffset {
			r.Timestamp = r.Timestamp.Add(time.Hour * time.Duration(defaultTimeOffset))
		}
	}
	return
}

type PartialEvent struct {
	Crid    string                 `json:"c"`
	Message string                 `json:"m"`
	Keyword string                 `json:"k"`
	Extra   map[string]interface{} `json:"x"`
}

func isV2Message(raw string) bool {
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

func decodeV2BeatMessage(raw string, r *Event) (ok bool) {
	// check length
	if len(raw) < 32 {
		return
	}

	// decode timestamp
	var err error
	if r.Timestamp, err = time.Parse(v2TimestampLayout, raw[1:30]); err != nil {
		return
	}

	// remaining
	buf := []byte(strings.TrimSpace(raw[31:]))

	if bytes.HasPrefix(buf, []byte("{")) && bytes.HasSuffix(buf, []byte("}")) {
		var p PartialEvent
		if err = json.Unmarshal(buf, &p); err != nil {
			return
		}
		r.Crid = p.Crid
		r.Message = p.Message
		r.Keyword = p.Keyword
		r.Extra = p.Extra
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

func decodeV1BeatMessage(raw string, isJSON bool, r *Event) (noOffset bool, ok bool) {
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
		if !decodeExtraStr(r.Extra, "topic", &r.Topic) {
			ok = false
			return
		}
		// optional extra 'project', 'crid'
		decodeExtraStr(r.Extra, "project", &r.Project)
		decodeExtraStr(r.Extra, "crid", &r.Crid)
		// optional extract 'timestamp'
		if decodeExtraTime(r.Extra, "timestamp", &r.Timestamp) {
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

func decodeBeatSource(raw string, r *Event) bool {
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

func decodeExtraStr(m map[string]interface{}, key string, out *string) bool {
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

func decodeExtraTime(m map[string]interface{}, key string, out *time.Time) bool {
	if m == nil || out == nil {
		return false
	}
	var tsStr string
	if decodeExtraStr(m, key, &tsStr) {
		if t, err := time.Parse(time.RFC3339, tsStr); err != nil {
			return false
		} else {
			*out = t // update if success
			return true
		}
	}
	return false
}
