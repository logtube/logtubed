package iocount

import "fmt"

func SimpleFormatByteSize(c int64) string {
	if c > 1000000000 {
		return fmt.Sprintf("%.02fgb", float64(c)/float64(1000000000))
	} else if c > 1000000 {
		return fmt.Sprintf("%.02fmb", float64(c)/float64(1000000))
	} else if c > 1000 {
		return fmt.Sprintf("%.02fkb", float64(c)/float64(1000))
	}
	return fmt.Sprintf("%db", c)
}
