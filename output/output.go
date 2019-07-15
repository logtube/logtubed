package output

// Operation is a JSON marshaled Event as its final format
type Operation interface {
	// Index of the event
	GetIndex() string

	// JSON marshaled body
	GetBody() []byte
}

// Output is a final Event storage target, use to be ES or Files
type Output interface {
	// This method output a Operation to storage target
	PutOperation(op Operation) error
}
