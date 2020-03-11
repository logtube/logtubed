package beat

import (
	"fmt"
	"github.com/logtube/logtubed/types"
	"github.com/stretchr/testify/require"
	"log"
	"sort"
	"strings"
	"testing"
)

func TestNginxPipeline_Format(t *testing.T) {
	var tags []string
	var items []string
	for tag := range ngxTagNames {
		tags = append(tags, tag)
	}

	sort.Strings(tags)

	for _, tag := range tags {
		items = append(items, fmt.Sprintf("%s=$%s", tag, ngxTagNames[tag]))
	}

	sb := &strings.Builder{}
	sb.WriteString("\nlog_format  compactkv '[$time_iso8601] ")
	sb.WriteString(strings.Join(items, " | "))
	sb.WriteString(`';`)
	t.Log(sb.String())
}

func TestDecodeNginxLog(t *testing.T) {
	m := &ngxPipeline{}
	var event types.Event
	event.Extra = map[string]interface{}{}
	ok := m.Process(Event{Message: "[2020-03-11T20:05:34+08:00] r=GET /hello%20world HTTP/1.1|ra=::1|urt=-|bbs=209|hua=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36|hxff=-|s=404|ua=-|hh=localhost|hr=-|rt=0.000"}, &event)
	log.Printf("%+v", event)
	require.True(t, ok)
}
