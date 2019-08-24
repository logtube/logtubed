package main

import "fmt"

type Result struct {
	Message string
	OK      bool
}

type Results struct {
	Results []Result
}

func (r *Results) Add(ok bool, s string, items ...interface{}) {
	r.Results = append(r.Results, Result{Message: fmt.Sprintf(s, items...), OK: ok})
}

func (r *Results) Passing(s string, items ...interface{}) {
	r.Add(true, s, items...)
}

func (r *Results) Failed(s string, items ...interface{}) {
	r.Add(false, s, items...)
}
