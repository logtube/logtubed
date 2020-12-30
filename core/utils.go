package core

import "strings"

func digestPathComponent(s string) string {
	if len(s) == 36 {
		if digestPathComponent(s[0:8]+s[9:13]+s[14:18]+s[19:23]+s[24:36]) == ":hex" {
			return ":uuid"
		}
	}
	var (
		dec      = true
		hex      = true
		float    = true
		foundDot = false
	)

	if len(s) != 16 && len(s) != 32 && len(s) != 64 {
		hex = false
	}

	for i, c := range s {
		if c == '.' {
			foundDot = true
		}
		if c == ',' {
			continue
		}
		if !((c >= '0' && c <= '9') || (c == '-' && i == 0)) {
			dec = false
		}
		if !((c >= '0' && c <= '9') || (c == '.' && i != 0 && i != len(s)-1) || (c == '-' && i == 0)) {
			float = false
		}
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			hex = false
		}
	}

	if !foundDot {
		float = false
	}

	if hex {
		return ":hex"
	}
	if float {
		return ":float"
	}
	if dec {
		return ":dec"
	}
	return s
}

func digestPath(p string) string {
	if p == "" {
		return p
	}
	inv := strings.Split(p, "/")
	if len(inv) == 1 {
		return p
	}
	ret := make([]string, 0, len(inv))
	for _, item := range inv {
		if item == "" {
			continue
		}
		ret = append(ret, digestPathComponent(item))
	}
	return "/" + strings.Join(ret, "/")
}
