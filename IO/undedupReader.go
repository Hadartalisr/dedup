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
	 if err != nil && err != io.ErrUnexpectedEOF {
	 	return nil, nil
	 }
	 length := 	binary.LittleEndian.Uint32(buf[4:8])
	data := buf[8:length+8]
	return &data, nil
}

func (undedupReader *UndedupReader) Close() {
	undedupReader.file.Seek(0,0)
	undedupReader.file.Close()
}
