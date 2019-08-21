package common

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

// A Capacity represents a size in bytes.
type Capacity uint64

// Common capacities.
const (
	Byte Capacity = 1

	Kibibyte = Byte << 10
	Mebibyte = Kibibyte << 10
	Gibibyte = Mebibyte << 10
	Tebibyte = Gibibyte << 10
	Pebibyte = Tebibyte << 10
	Exbibyte = Pebibyte << 10

	Kilobyte = Byte * 1000
	Megabyte = Kilobyte * 1000
	Gigabyte = Megabyte * 1000
	Terabyte = Gigabyte * 1000
	Petabyte = Terabyte * 1000
	Exabyte  = Petabyte * 1000
)

type capacityUnit struct {
	s string   // suffix
	d Capacity // decimal
	b Capacity // binary
}

var (
	capacityUnits = [...]capacityUnit{
		{s: "k", d: Kilobyte, b: Kibibyte},
		{s: "m", d: Megabyte, b: Mebibyte},
		{s: "g", d: Gigabyte, b: Gibibyte},
		{s: "t", d: Terabyte, b: Tebibyte},
		{s: "p", d: Petabyte, b: Pebibyte},
		{s: "e", d: Exabyte, b: Exbibyte},
	}

	capacityUnitMap = map[string]Capacity{}

	capacityPattern = regexp.MustCompile(`([0-9]+)(\.([0-9]+))?\s?([a-z-A-Z]+)?`)
)

func init() {
	// build capacityUnitMap
	for _, unit := range capacityUnits {
		capacityUnitMap[unit.s] = unit.d
		capacityUnitMap[unit.s+"b"] = unit.d
		capacityUnitMap[unit.s+"i"] = unit.b
		capacityUnitMap[unit.s+"ib"] = unit.b
	}
	capacityUnitMap[""] = Byte
	capacityUnitMap["b"] = Byte
}

func capacityExp(a, n uint64) uint64 {
	ret := uint64(1)
	for i := n; i > 0; i >>= 1 {
		if i&1 != 0 {
			ret *= a
		}
		a *= a
	}
	return ret
}

func ParseCapacity(s string) (Capacity, error) {
	res := capacityPattern.FindStringSubmatch(s)
	if len(res) != 5 {
		return 0, errors.New("invalid capacity: " + s)
	}
	bs, es, us := res[1], res[3], res[4]
	var err error
	var u Capacity
	if u, err = ParseCapacityUnit(us); err != nil {
		return 0, err
	}
	var b uint64
	if b, err = strconv.ParseUint(bs, 10, 64); err != nil {
		return 0, err
	}
	val := b * uint64(u)
	if r := len(es); r > 0 {
		var e uint64
		if e, err = strconv.ParseUint(es, 10, 64); err != nil {
			return 0, nil
		}
		val = val + e*uint64(u)/capacityExp(10, uint64(r))
	}
	return Capacity(val), nil
}

func ParseCapacityUnit(s string) (Capacity, error) {
	s = strings.ToLower(s)
	if val, ok := capacityUnitMap[s]; ok {
		return val, nil
	} else {
		return 0, errors.New("unknown capacity unit: " + s)
	}
}
