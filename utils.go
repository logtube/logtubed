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

func extractIP(addr string) string {
	c := strings.Split(addr, ":")
	if len(c) < 2 {
		return "UNKNOWN"
	} else if len(c) == 2 {
		return c[0]
	} else {
		return strings.Join(c[0:len(c)-1], ":")
	}
}
