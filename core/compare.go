package core

import (
	"bytes"
	"io"
	"log"
	"os"
)

var good  = 0
var bad  = 0
var badIndices = make([]int, 1)

func Compare(inputFilePath, outputFilePath *string) {
	equal := equal(*inputFilePath, *outputFilePath)
	println("good ", good)
	println("bad ", bad)
	println()
	if !equal {
		print(":(")
		return
	}
	print(":)")
}

func equal(file1, file2 string) bool {
	// Check file size ...
	chunkSize := /*config.MinChunkSizeInBytes*/  1000
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
				return true
			} else if err1 == io.EOF || err2 == io.EOF {
				return false
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			//return false
			bad++
			println(int(offset))
		} else  {
			good++
		}
	}
}