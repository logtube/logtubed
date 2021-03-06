package core

import "testing"

func Test_digestPath(t *testing.T) {
	type args struct {
		p string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test-clean-slash",
			args: args{p: "//hello//world/"},
			want: "/hello/world",
		},
		{
			name: "test-clean-number",
			args: args{p: "//hello/world/-1/223/5333/store"},
			want: "/hello/world/:dec/:dec/:dec/store",
		},
		{
			name: "test-clean-number-coma",
			args: args{p: "//hello/world/-1/223/5333,224,553/store"},
			want: "/hello/world/:dec/:dec/:dec/store",
		},
		{
			name: "test-clean-hex",
			args: args{p: "//hello/world/-1/ee3c5e83670ba40dd80d74d7773e309b/store"},
			want: "/hello/world/:dec/:hex/store",
		},
		{
			name: "test-clean-uuid",
			args: args{p: "//v1/goods/detail/126602/1441/85978/A54DC252-5DD6-4128-A8DA-BE13D9D7CBDB"},
			want: "/v1/goods/detail/:dec/:dec/:dec/:uuid",
		},
		{
			name: "test-clean-float",
			args: args{p: "//v1/goods/detail/126602/1441/85978/233.443"},
			want: "/v1/goods/detail/:dec/:dec/:dec/:float",
		},
		{
			name: "test-clean-float-1",
			args: args{p: "//v1/goods/detail/126602/1441/85978/233.443,333.44"},
			want: "/v1/goods/detail/:dec/:dec/:dec/:float",
		},
		{
			name: "test-clean-float-1",
			args: args{p: "//v1/goods/detail/126602/1441/85978/.11"},
			want: "/v1/goods/detail/:dec/:dec/:dec/.11",
		},
		{
			name: "test-clean-float-1",
			args: args{p: "//v1/goods/detail/126602/1441/85978/11."},
			want: "/v1/goods/detail/:dec/:dec/:dec/11.",
		},
		{
			name: "test-clean-version",
			args: args{p: "//v1/goods/detail/126602/1441/85978/11.2.3"},
			want: "/v1/goods/detail/:dec/:dec/:dec/:version",
		},
		{
			name: "test-no-touch-dubbo",
			args: args{p: "com.something.else"},
			want: "com.something.else",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := digestPath(tt.args.p); got != tt.want {
				t.Errorf("digestPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
