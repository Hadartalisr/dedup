package utils

import "strconv"

func Uint64ToString(myInt uint64) string {
	return strconv.FormatUint(uint64(myInt), 10)
}
