package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"time"
)

type ESCleanScript struct {
	Endpoint string
	TotalGB  int64
	TargetGB int64
	Indices  []string
}

func (d ESCleanScript) WriteTo(file string) (err error) {
	buf := &bytes.Buffer{}
	_, _ = fmt.Fprintf(buf, "# generated at %s\n", time.Now().String())
	_, _ = fmt.Fprintf(buf, "# total %d Gb, clean %d Gb\n", d.TotalGB, d.TargetGB)
	for _, idx := range d.Indices {
		_, _ = fmt.Fprintf(buf, "curl -XDELETE http://%s/%s\n", d.Endpoint, idx)
	}
	err = ioutil.WriteFile(file, buf.Bytes(), 0755)
	return
}
