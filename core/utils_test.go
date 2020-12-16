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
