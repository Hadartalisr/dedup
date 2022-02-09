package core

import (
	"Deduper/config"
	"bytes"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"os"
)

var equalSlices = 0
var unequalSlices  = 0

func Compare(inputFilePath, outputFilePath *string) {
	equal(*inputFilePath, *outputFilePath)
	logrus.Info("equalSlices ", equalSlices)
	logrus.Info("unequalSlices ", unequalSlices)
	if unequalSlices > 0 {
		logrus.Info(":(")
		return
	}
	logrus.Info(":)")
}

func equal(file1, file2 string) {
	chunkSize := config.MinChunkSizeInBytes
	f1, err := os.Open(file1)
	if err != nil {
		log.Fatal(err)
	}
	defer f1.Close()

	f2, err := os.Open(file2)
	if err != nil {
		log.Fatal(err)
	}
	defer f2.Close()

	for {
		offset, _ := f1.Seek(0, io.SeekCurrent)
		b1 := make([]byte, chunkSize)
		_, err1 := f1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := f2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return
			} else if err1 == io.EOF || err2 == io.EOF {
				return
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			unequalSlices++
			println(int(offset))
		} else  {
			equalSlices++
		}
	}
}