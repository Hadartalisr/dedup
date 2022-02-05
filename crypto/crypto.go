package crypto

import (
	"hash/crc32"
)

var crc32Table = crc32.MakeTable(0xD5828281)

func Checksum(bytes []byte) uint32 {
	return crc32.Checksum(bytes, crc32Table)
}
