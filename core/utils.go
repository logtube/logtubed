package core

import "strings"

func isNum(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || c == '-') {
			return false
		}
	}
	return true
}

func isHex(s string) bool {
	if len(s) != 16 && len(s) != 32 && len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

func isUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	s = s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:36]
	return isHex(s)
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
		if strings.Contains(item, ",") {
			item = ":dec"
		} else if isNum(item) {
			item = ":dec"
		} else if isHex(item) {
			item = ":hex"
		} else if isUUID(item) {
			item = ":uuid"
		}
		ret = append(ret, item)
	}
	return "/" + strings.Join(ret, "/")
}
