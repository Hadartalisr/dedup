package crypto

import (
	"crypto/sha1"
	"encoding/base64"
)

func Checksum(bytes []byte) string {
	hasher := sha1.New()
	hasher.Write(bytes)
	sha := base64.URLEncoding.EncodeToString(hasher.Sum(nil))
	return sha
}
