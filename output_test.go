package main

import (
	"errors"
	"testing"
)

type testOutput struct {
	c int
	s bool
}

func (t *testOutput) Close() error {
	t.s = true
	return errors.New("dummy")
}

func (t *testOutput) Put(e Operation) error {
	t.c++
	return errors.New("dummy")
}

func TestMultiOutput(t *testing.T) {
	o1 := &testOutput{}
	o2 := &testOutput{}

	o := MultiOutput(o1, o2)

	o.Put(Operation{})
	o.Put(Operation{})
	o.Put(Operation{})

	o.Close()

	if !o1.s {
		t.Fatal("o1 not closed")
	}
	if !o2.s {
		t.Fatal("o2 not closed")
	}
	if o1.c != 3 {
		t.Fatal("o1 not 3")
	}
	if o2.c != 3 {
		t.Fatal("o2 not 3")
	}
}
