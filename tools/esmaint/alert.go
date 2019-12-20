package main

import (
	"go.guoyk.net/common"
	"log"
)

func sendAlert(url string, message string) {
	log.Printf("send: %q", message)
	if len(url) > 0 {
		_ = common.PostJSON(url, map[string]string{
			"source":  "esmaint",
			"message": message,
		}, nil)
	}
}
