package test

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

func Equal(inputFilePath, outputFilePath string) (bool, error){
	inputFile, err := os.Open(inputFilePath)
	outputFile, err := os.Open(outputFilePath)
	readBufferSizeInBytes := 4*200
	inputBuf := make([]byte, 0, readBufferSizeInBytes)
	outputBuf := make([]byte, 0, readBufferSizeInBytes)

	for {
		// reading the inputFile in buffer
		n, err := inputFile.Read(inputBuf[:cap(inputBuf)])
		inputBuf = inputBuf[:n]
		if n == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			return false, err
		}
		if err != nil && err != io.EOF {
			return false, err
		}
		// reading the outputFile in buffer
		m, err := outputFile.Read(outputBuf[:cap(outputBuf)])
		outputBuf = outputBuf[:m]
		if m == 0 {
			if err == nil {
				continue
			}
			if err == io.EOF {
				break
			}
			return false, err
		}
		if err != nil && err != io.EOF {
			return false, err
		}

		equal := bytes.Compare(inputBuf, outputBuf)
		if equal != 0 {
			fmt.Println("!..Slices are not equal..!")
			return false, err
		}

	}
	fmt.Println("!..Files are equal..!")
	return true, err
}