package main

// Output output interface, all outputs are operated single-thread
type Output interface {
	PutOperation(op Operation) error
	Close() error
}

type MultiOutput []Output

func (m MultiOutput) Close() (err error) {
	if len(m) == 1 {
		return m[0].Close()
	}
	for _, o := range m {
		err = combineError(err, o.Close())
	}
	return
}

func (m MultiOutput) PutOperation(op Operation) (err error) {
	if len(m) == 1 {
		return m[0].PutOperation(op)
	}
	for _, o := range m {
		err = combineError(err, o.PutOperation(op))
	}
	return
}
