package IO

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type UndedupReader struct {
	file *os.File
	chunkMaxSize int
	//TODO cache
}

func NewUndedupFileReader(filePath string, chunkMaxSize int) (*UndedupReader, error) {
	inputFile, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	undedupReader := UndedupReader{
		file: inputFile,
		chunkMaxSize: chunkMaxSize,
	}
	return &undedupReader, nil
}

func (undedupReader *UndedupReader) GetChunk(offset int) (*[]byte, error) {
	 _, err := undedupReader.file.Seek(int64(offset),0)
	 if err != nil {
	 	//TODO handle
	 }
	 reader := bufio.NewReader(undedupReader.file)
	 buf := make([]byte, 4+undedupReader.chunkMaxSize)
	 _, err = io.ReadAtLeast(reader, buf, 4+undedupReader.chunkMaxSize)
	 length := 	binary.LittleEndian.Uint32(buf[:4])
	data := buf[4:length]
	return &data, err
}

func (undedupReader *UndedupReader) Close() {
	undedupReader.file.Close()
}
