package common

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

var (
	DefaultHTTPClient *http.Client
)

func init() {
	DefaultHTTPClient = &http.Client{
		Timeout: time.Second * 10,
	}
}

func httpJSON(method string, url string, in interface{}, out interface{}) (err error) {
	// body
	var body io.Reader
	if in != nil {
		var buf []byte
		if buf, err = json.Marshal(in); err != nil {
			return
		}
		body = bytes.NewReader(buf)
	}
	// request
	var req *http.Request
	if req, err = http.NewRequest(method, url, body); err != nil {
		return
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	// response
	var resp *http.Response
	if resp, err = DefaultHTTPClient.Do(req); err != nil {
		return
	}
	defer resp.Body.Close()
	// unmarshal
	if out != nil {
		var buf []byte
		if buf, err = ioutil.ReadAll(resp.Body); err != nil {
			return
		}
		err = json.Unmarshal(buf, out)
	}
	return
}

func GetJSON(url string, out interface{}) error {
	return httpJSON(http.MethodGet, url, nil, out)
}

func PostJSON(url string, in, out interface{}) error {
	return httpJSON(http.MethodPost, url, in, out)
}

func PutJSON(url string, in, out interface{}) error {
	return httpJSON(http.MethodPut, url, in, out)
}

func PatchJSON(url string, in, out interface{}) error {
	return httpJSON(http.MethodPatch, url, in, out)
}

func DeleteJSON(url string, out interface{}) error {
	return httpJSON(http.MethodDelete, url, nil, out)
}
