package main

import "testing"

func TestLoadOptionsFile(t *testing.T) {
	opt, err := loadOptionsFile("systemd/logtubed.yml")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(opt)
}
