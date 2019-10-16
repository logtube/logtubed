package beat

import (
	"github.com/logtube/logtubed/types"
	"github.com/stretchr/testify/require"
	"log"
	"testing"
)

func TestDecodeMySQLError(t *testing.T) {
	m := &mySQLErrorPipeline{}
	var event types.Event
	event.Extra = map[string]interface{}{}
	ok := m.decodeMySQLError(Event{Message: "2019-10-15T07:21:09.037619Z 0 [Note] Event Scheduler: Loaded 0 events"}, &event)
	log.Printf("%+v", event)
	require.True(t, ok)
}
