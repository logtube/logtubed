package main

import "testing"

func TestFormatStatsTime(t *testing.T) {
	if formatStatsTime(-618) != "-10:18" {
		t.Fatal("bad")
	}
}
