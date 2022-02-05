package IO

import (
	"bufio"
	"os"
)

func InitDedupFileReader(filePath string) (*os.File, *bufio.Reader, error) {
	inputFile, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}
	reader := bufio.NewReader(inputFile)
	return inputFile, reader, nil
}

func CloseFile(file *os.File) error {
	file.Close()
	return nil
}