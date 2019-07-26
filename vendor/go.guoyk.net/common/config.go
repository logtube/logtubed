package common

import (
	"encoding/json"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

func LoadYAMLConfigFile(file string, out interface{}) (err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(file); err != nil {
		return
	}
	if err = LoadYAMLConfig(buf, out); err != nil {
		return
	}
	return
}

func LoadYAMLConfig(buf []byte, out interface{}) (err error) {
	if err = yaml.Unmarshal(buf, out); err != nil {
		return
	}
	if err = SetDefaults(out); err != nil {
		return
	}
	return
}

func LoadJSONConfigFile(file string, out interface{}) (err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(file); err != nil {
		return
	}
	if err = LoadJSONConfig(buf, out); err != nil {
		return
	}
	return
}

func LoadJSONConfig(buf []byte, out interface{}) (err error) {
	if err = json.Unmarshal(buf, out); err != nil {
		return
	}
	if err = SetDefaults(out); err != nil {
		return
	}
	return
}
