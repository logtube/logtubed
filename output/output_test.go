package output

type testOperation struct {
	index string
	body  []byte
}

func (o *testOperation) GetIndex() string {
	return o.index
}

func (o *testOperation) GetBody() []byte {
	return o.body
}

