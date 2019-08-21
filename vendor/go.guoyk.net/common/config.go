package common

import (
	"encoding/json"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

func LoadYAMLConfigFile(file string, out interface{}) error {
	return ReadYAMLConfigFile(file, out)
}

func LoadYAMLConfig(buf []byte, out interface{}) error {
	return UnmarshalYAMLConfig(buf, out)
}

func LoadJSONConfigFile(file string, out interface{}) error {
	return ReadJSONConfigFile(file, out)
}

func LoadJSONConfig(buf []byte, out interface{}) error {
	return UnmarshalJSONConfig(buf, out)
}

func ReadYAMLConfigFile(file string, out interface{}) (err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(file); err != nil {
		return
	}
	if err = UnmarshalYAMLConfig(buf, out); err != nil {
		return
	}
	return
}

func UnmarshalYAMLConfig(buf []byte, out interface{}) (err error) {
	if err = yaml.Unmarshal(buf, out); err != nil {
		return
	}
	if err = SetDefaults(out); err != nil {
		return
	}
	return
}

func ReadJSONConfigFile(file string, out interface{}) (err error) {
	var buf []byte
	if buf, err = ioutil.ReadFile(file); err != nil {
		return
	}
	if err = UnmarshalJSONConfig(buf, out); err != nil {
		return
	}
	return
}

func UnmarshalJSONConfig(buf []byte, out interface{}) (err error) {
	if err = json.Unmarshal(buf, out); err != nil {
		return
	}
	if err = SetDefaults(out); err != nil {
		return
	}
	return
}
