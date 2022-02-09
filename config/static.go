package config

import "github.com/sirupsen/logrus"

// const variables
const (
	LogLevel = logrus.InfoLevel
	StartLength = 5
	MinChunkSizeInBytes  = 8 * 1024
	MaxChunkSizeInBytes   = 32 * 1024
	MaxChunksInWriterBuffer   = 3000
	ReadBufferSizeInBytes = MaxChunkSizeInBytes * 100
	CacheSize = 200
)
