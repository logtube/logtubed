package main

import "testing"

func TestFormatStatsTime(t *testing.T) {
	if statsTimeFormat(-618) != "-10:18" {
		t.Fatal("bad")
	}
}
