package output

import (
	"github.com/pkg/errors"
	"testing"
)

type multiTestOutput struct {
	fail bool
	ops  []Operation
}

func (o *multiTestOutput) PutOperation(op Operation) error {
	if o.fail {
		return errors.New("oh")
	}
	o.ops = append(o.ops, op)
	return nil
}

func TestMultiOutput_PutOperation(t *testing.T) {
	var err error
	mo := MultiOutput{}
	mo = append(mo, &multiTestOutput{fail: true})
	mo = append(mo, &multiTestOutput{fail: true})

	err = mo.PutOperation(&testOperation{index: "hello", body: []byte("world")})
	if err == nil {
		t.Fatal("shall error")
	}
	if err.Error() != "oh; oh" {
		t.Fatal("error not combined")
	}

	mo = MultiOutput{}
	mo = append(mo, &multiTestOutput{})
	mo = append(mo, &multiTestOutput{fail: true})

	err = mo.PutOperation(&testOperation{index: "hello", body: []byte("world")})
	if err == nil {
		t.Fatal("shall error")
	}
	if err.Error() != "oh" {
		t.Fatal("error not good")
	}
	if mo[0].(*multiTestOutput).ops[0].GetIndex() != "hello" {
		t.Fatal("not append")
	}
}
