package types

import (
	"os"
	"testing"
)

func TestLoadOptionsFile(t *testing.T) {
	_ = os.Setenv("LOGTUBED_ES_CONCURRENCY", "9")
	opt, err := LoadOptions("../misc/logtubed.yml")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", opt)
	if opt.OutputES.Concurrency != 9 {
		t.Fatal("env not applied")
	}
}
