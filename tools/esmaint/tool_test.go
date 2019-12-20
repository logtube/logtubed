package main

import (
	"reflect"
	"testing"
	"time"
)

func Test_dateFromIndex(t *testing.T) {
	type args struct {
		index string
	}
	tests := []struct {
		name     string
		args     args
		wantDate time.Time
		wantOk   bool
	}{
		{
			name:     "basic ok",
			args:     args{index: "x-xxx-xxx-2019-08-30"},
			wantDate: time.Date(2019, time.August, 30, 0, 0, 0, 0, time.UTC),
			wantOk:   true,
		},
		{
			name:   "basic not ok",
			args:   args{index: "x-xxx-xxx-2019-8-30"},
			wantOk: false,
		},
		{
			name:   "basic short",
			args:   args{index: "x-xxx-xxx-2019-8-30"},
			wantOk: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDate, gotOk := dateFromIndex(tt.args.index)
			if !reflect.DeepEqual(gotDate, tt.wantDate) {
				t.Errorf("dateFromIndex() gotDate = %v, want %v", gotDate, tt.wantDate)
			}
			if gotOk != tt.wantOk {
				t.Errorf("dateFromIndex() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}
