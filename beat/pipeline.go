package beat

import "github.com/logtube/logtubed/types"

type Pipeline interface {
	Name() string
	Match(b Event) bool
	Process(b Event, e *types.Event) (success bool)
}
