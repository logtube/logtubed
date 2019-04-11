package main

import (
	"reflect"
	"testing"
	"time"
)

func TestCompactEvent_ToEvent(t *testing.T) {
	type fields struct {
		Timestamp int64
		Hostname  string
		Env       string
		Project   string
		Topic     string
		Crid      string
		Message   string
		Keyword   string
		Extra     map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		wantE  Event
	}{
		{
			name:   "case-1",
			fields: fields{1000, "a", "b", "c", "d", "e", "f", "g", map[string]interface{}{"h": "i"}},
			wantE:  Event{time.Unix(1, 0), "a", "b", "c", "d", "e", "f", "g", map[string]interface{}{"h": "i"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CompactEvent{
				Timestamp: tt.fields.Timestamp,
				Hostname:  tt.fields.Hostname,
				Env:       tt.fields.Env,
				Project:   tt.fields.Project,
				Topic:     tt.fields.Topic,
				Crid:      tt.fields.Crid,
				Message:   tt.fields.Message,
				Keyword:   tt.fields.Keyword,
				Extra:     tt.fields.Extra,
			}
			if gotE := c.ToEvent(); !reflect.DeepEqual(gotE, tt.wantE) {
				t.Errorf("CompactEvent.ToEvent() = %v, want %v", gotE, tt.wantE)
			}
		})
	}
}

func TestUnmarshalCompactEventJSON(t *testing.T) {
	type args struct {
		buf []byte
	}
	tests := []struct {
		name    string
		args    args
		wantC   CompactEvent
		wantErr bool
	}{
		{
			name:    "case-1",
			args:    args{[]byte(`{"t":1000, "h":"a", "e":"b", "p":"c", "o":"d", "c":"e", "m":"f", "k":"g", "x":{"h":"i"}}`)},
			wantC:   CompactEvent{1000, "a", "b", "c", "d", "e", "f", "g", map[string]interface{}{"h": "i"}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotC, err := UnmarshalCompactEventJSON(tt.args.buf)
			if (err != nil) != tt.wantErr {
				t.Errorf("UnmarshalCompactEventJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotC, tt.wantC) {
				t.Errorf("UnmarshalCompactEventJSON() = %v, want %v", gotC, tt.wantC)
			}
		})
	}
}
