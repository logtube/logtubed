package compactkv

import (
	"strconv"
	"strings"
)

type ValueType int

const (
	StringType ValueType = iota
	IntegerType
	FloatType
)

const (
	Separator = "|"
)

type CompactKV struct {
	TagNames map[string]string
	TagTypes map[string]ValueType
}

func NewCompactKV() *CompactKV {
	return &CompactKV{
		TagNames: map[string]string{},
		TagTypes: map[string]ValueType{},
	}
}

func (ckv *CompactKV) Add(tag string, name string, typ ValueType) {
	tag = strings.ToLower(tag)
	ckv.TagNames[tag] = name
	ckv.TagTypes[tag] = typ
}

func (ckv *CompactKV) Parse(str string) map[string]interface{} {
	ret := map[string]interface{}{}
	items := strings.Split(str, Separator)
	for _, item := range items {
		kv := strings.SplitN(item, "=", 2)
		if len(kv) != 2 {
			continue
		}
		tag := strings.ToLower(strings.TrimSpace(kv[0]))
		val := strings.TrimSpace(kv[1])
		if len(tag) == 0 || len(val) == 0 {
			continue
		}
		name := ckv.TagNames[tag]
		if len(name) == 0 {
			name = tag
		}
		switch ckv.TagTypes[tag] {
		case StringType:
			ret[name] = val
		case IntegerType:
			valInt, _ := strconv.ParseInt(val, 10, 64)
			ret[name] = valInt
		case FloatType:
			valFloat, _ := strconv.ParseFloat(val, 64)
			ret[name] = valFloat
		default:
		}
	}
	return ret
}
