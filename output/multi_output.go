package output

import (
	"github.com/logtube/logtubed/utils/egroup"
)

type MultiOutput []Output

func (m MultiOutput) PutOperation(op Operation) error {
	if len(m) == 0 {
		return nil
	}
	if len(m) == 1 {
		return m[0].PutOperation(op)
	}
	eg := egroup.New()
	for _, o := range m {
		eg.Put(o.PutOperation(op))
	}
	return eg.Err()
}
