package main

import "time"

func dateFromIndex(index string) (date time.Time, ok bool) {
	// example: x-xxx-xxxxx-2019-08-31
	if len(index) < len(indexDateLayout) {
		return
	}
	var err error
	if date, err = time.Parse(indexDateLayout, index[len(index)-len(indexDateLayout):]); err != nil {
		return
	}
	ok = true
	return
}
