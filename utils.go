package main

import (
	"strings"
)

func stringSliceContainsIgnoreCase(s []string, t string) bool {
	for _, r := range s {
		if strings.ToLower(r) == strings.ToLower(t) {
			return true
		}
	}
	return false
}
