package main

import (
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

var (
	optBind      string
	optPath      string
	optEndpoints string
)

func main() {
	flag.StringVar(&optBind, "b", ":8090", "bind address")
	flag.StringVar(&optPath, "p", "/logtubedash/api/stats", "path")
	flag.StringVar(&optEndpoints, "e", "", "endpoints, comma separated, http://10.10.10.10:6060/stats")
	flag.Parse()

	http.HandleFunc(optPath, route)
	_ = http.ListenAndServe(optBind, nil)
}

func getJSON(url string, out interface{}) (err error) {
	var resp *http.Response
	if resp, err = http.Get(url); err != nil {
		return
	}
	defer resp.Body.Close()
	var buf []byte
	if buf, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	err = json.Unmarshal(buf, out)
	return
}

func route(rw http.ResponseWriter, r *http.Request) {
	var err error
	var out []map[string]interface{}
	endpoints := strings.Split(optEndpoints, ",")
	for _, endpoint := range endpoints {
		var stat map[string]interface{}
		if err = getJSON(endpoint, &stat); err != nil {
			rw.Header().Set("Content-Type", "text/plain")
			rw.WriteHeader(http.StatusInternalServerError)
			io.WriteString(rw, err.Error())
			return
		}
		out = append(out, stat)
	}
	var buf []byte
	if buf, err = json.Marshal(out); err != nil {
		rw.Header().Set("Content-Type", "text/plain")
		rw.WriteHeader(http.StatusInternalServerError)
		io.WriteString(rw, err.Error())
		return
	}
	rw.Header().Set("Content-Type", "application/json")
	rw.Write(buf)
}
