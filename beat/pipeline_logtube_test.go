package beat

import (
	"github.com/logtube/logtubed/types"
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
			if got := isLogtubeV2Message(tt.args.raw); got != tt.want {
				t.Errorf("isLogtubeV2Message() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_decodeV2BeatMessage(t *testing.T) {
	var r types.Event
	if !decodeLogtubeV2BeatMessage("[2019-03-27 12:32:22.324 +0800] CRID[0123456780af] KEYWORD[keyword1,keyword2] message body", &r) {
		t.Fatal("failed to decode plain")
	}
	if r.Crid != "0123456780af" {
		t.Fatal("failed to decode plain crid")
	}
	if !decodeLogtubeV2BeatMessage("[2019-03-27 12:32:22.324 +0800] {\"c\":\"0123456780af\", \"k\":\"keyword1,keyword2\", \"x\":{ \"duration\": 121 }}", &r) {
		t.Fatal("failed to decode plain")
	}
	if r.Crid != "0123456780af" {
		t.Fatal("failed to decode json crid")
	}
}

func Test_decodeV2_1BeatMessage(t *testing.T) {
	var r types.Event
	if !decodeLogtubeV2BeatMessage(`[2019-03-27 12:32:22.324 +0800] [{"c":"0123456780af","s":"test-source","k":"hello,world","x":{"key1":"val1"}}] message body`, &r) {
		t.Fatal("failed to decode plain")
	}
	if r.Crid != "0123456780af" {
		t.Fatal("failed to decode plain crid")
	}
	if r.Keyword != "hello,world" {
		t.Fatal("failed to decode keyword")
	}
	if r.Extra["key1"] != "val1" {
		t.Fatal("failed to decode extra")
	}
	if r.Message != "message body" {
		t.Fatal("failed to decode message")
	}
	if r.Crsrc != "test-source" {
		t.Fatal("failed to decode crsrc")
	}

	if !decodeLogtubeV2BeatMessage(`[2020-08-14 19:09:21.727 +0800] [{"c":"995799fb55e0d7ee","x":{"method":"POST","host":"h5-web.test.pagoda.com.cn","query":"","header_user_token":"","header_app_info":"{\"channel\":\"wx\",\"openId\":\"owJnujn_rz7Bny-tnire7fNe1GbA\"}","duration":0,"response_size":false,"status":200}}]
`, &r) {
		t.Fatal("failed to decode")
	}

	t.Log(r)
}

func Test_decodeBeatSource(t *testing.T) {
	var e types.Event
	var ok bool

	ok = decodeLogtubeBeatSource("/home/tomcat/prod/x-redis-track/ms-order.20190022.log", &e)
	if !ok {
		t.Fatal("not ok")
	}
	if e.Env != "prod" {
		t.Fatal("bad env")
	}
	if e.Topic != "x-redis-track" {
		t.Fatal("bad topic")
	}
	if e.Project != "ms-order" {
		t.Fatal("bad project")
	}

	ok = decodeLogtubeBeatSource("/home/tomcat/prod/x-redis-track/test.x-mybatis-track.ms-order.20190022.log", &e)
	if !ok {
		t.Fatal("not ok")
	}
	if e.Env != "test" {
		t.Fatal("bad env")
	}
	if e.Topic != "x-mybatis-track" {
		t.Fatal("bad topic")
	}
	if e.Project != "ms-order" {
		t.Fatal("bad project")
	}

	ok = decodeLogtubeBeatSource("/home/tomcat/prod/x-redis-track/test.x-mybatis-track.ms-order.log", &e)
	if !ok {
		t.Fatal("not ok")
	}
	if e.Env != "test" {
		t.Fatal("bad env")
	}
	if e.Topic != "x-mybatis-track" {
		t.Fatal("bad topic")
	}
	if e.Project != "ms-order" {
		t.Fatal("bad project")
	}

}
