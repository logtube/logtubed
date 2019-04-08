package main

import (
	"testing"
)

func Test_isV2Message(t *testing.T) {
	type args struct {
		raw string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "case-1",
			args: args{raw: "[2018-09-10 17:24:22.120 +0800]"},
			want: true,
		},
		{
			name: "case-2",
			args: args{raw: "[2018/09/10 17:24:22.120 +0800]"},
			want: false,
		},
		{
			name: "case-3",
			args: args{raw: "[2018-09-10 17:24:22.120 -0800]"},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isV2Message(tt.args.raw); got != tt.want {
				t.Errorf("isV2Message() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_decodeV2BeatMessage(t *testing.T) {
	var r Event
	if !decodeV2BeatMessage("[2019-03-27 12:32:22.324 +0800] CRID[0123456780af] KEYWORD[keyword1,keyword2] message body", &r) {
		t.Fatal("failed to decode plain")
	}
	if r.Crid != "0123456780af" {
		t.Fatal("failed to decode crid")
	}
	t.Log(r)
	if !decodeV2BeatMessage("[2019-03-27 12:32:22.324 +0800] {\"c\":\"0123456780af\", \"k\":\"keyword1,keyword2\", \"x\":{ \"duration\": 121 }}", &r) {
		t.Fatal("failed to decode plain")
	}
	if r.Crid != "0123456780af" {
		t.Fatal("failed to decode json")
	}
	t.Log(r)
}
