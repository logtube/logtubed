package main

import (
	"os"
	"testing"
)

func TestLoadOptionsFile(t *testing.T) {
	_ = os.Setenv("LOGTUBED_ES_CONCURRENCY", "9")
	opt, err := LoadOptions("contrib/logtubed.yml")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(opt)
	if opt.OutputES.Concurrency != 9 {
		t.Fatal("env not applied")
	}
}
