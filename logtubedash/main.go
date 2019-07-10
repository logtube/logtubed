package main

import (
	"encoding/json"
	"flag"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"go.guoyk.net/binfs/binfsecho"
	"io/ioutil"
	"net/http"
	"strings"
)

var (
	optBind      string
	optEndpoints string
)

const (
	PREFIX = "/logtubedash"
)

func main() {
	flag.StringVar(&optBind, "b", ":8090", "bind address")
	flag.StringVar(&optEndpoints, "e", "http://127.0.0.1:6060/stats", "endpoints, comma separated, http://127.0.0.1:6060/stats")
	flag.Parse()

	e := echo.New()
	e.HidePort = true
	e.HideBanner = true
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Static(PREFIX, "public")
	e.Use(binfsecho.StaticWithConfig(binfsecho.StaticConfig{Prefix: PREFIX, Root: "public",}))
	e.GET(PREFIX+"/api/stats", route)
	e.Start(optBind)
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

func route(ctx echo.Context) (err error) {
	var out []map[string]interface{}
	endpoints := strings.Split(optEndpoints, ",")
	for _, endpoint := range endpoints {
		endpoint = strings.TrimSpace(endpoint)
		var stat map[string]interface{}
		if err = getJSON(endpoint, &stat); err != nil {
			return
		}
		out = append(out, stat)
	}
	return ctx.JSON(http.StatusOK, out)
}
