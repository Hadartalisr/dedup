package IO

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/sirupsen/logrus"
	"os"
)

type DedupWriter struct {
	OutputFile *os.File
	CurrentOffset int
	batchCounter int
	maxBatch int
	buffer *bytes.Buffer
	writer *bufio.Writer
}

func NewDedupWriter(filePath *string, maxChunksInBatch, chunkMaxSize int) (*DedupWriter, error) {
	outputFile, err := os.Create(*filePath)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(outputFile)
	buf := &bytes.Buffer{}
	buf.Grow(chunkMaxSize * maxChunksInBatch)
	dedupWriter := DedupWriter{
		outputFile,
		0,
		0,
		maxChunksInBatch,
		buf,
		writer,
	}
	return &dedupWriter, err
}

func (dedupWriter *DedupWriter) Close()  error {
	dedupWriter.OutputFile.Seek(0,0)
	dedupWriter.OutputFile.Close()
	return nil
}

func (dedupWriter *DedupWriter) WriteBlank(data *[]byte) (int, error) {
	dedupWriter.CurrentOffset += len(*data)
	return dedupWriter.buffer.Write(*data)
}

// WriteData
// return the number of bytes which were written (4 + length of data)
func (dedupWriter *DedupWriter) WriteData(data *[]byte) (int, error) {
	if dedupWriter.batchCounter > dedupWriter.maxBatch {
		dedupWriter.FlushData()
	}
	// calculate length
	length := len(*data)
	bytesToWrite := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesToWrite, uint32(length))
	bytesToWrite = append(bytesToWrite, *data...) // write length is 4 bytes + len(data)
	dedupWriter.batchCounter++
	dedupWriter.buffer.Write(bytesToWrite)
	return len(bytesToWrite), nil
}

func (dedupWriter *DedupWriter) WriteMataData(offsetsArr []int) (int, error) {
	lengthBytes:= make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBytes, uint32(len(offsetsArr)))
	dedupWriter.WriteBlank(&lengthBytes)
	for _, offset := range offsetsArr {
		bytesToWrite := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytesToWrite, uint32(offset))
		dedupWriter.WriteBlank(&bytesToWrite)
	}
	return len(offsetsArr) + 4, nil
}

func (dedupWriter *DedupWriter) WriteMataDataOffset(offset int) error {
	logrus.Debugf("WriteMataDataOffset %d \n", offset)
	bytesToWrite := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesToWrite, uint32(offset))
	dedupWriter.OutputFile.Seek(0,0)
	ioWriter :=  bufio.NewWriter(dedupWriter.OutputFile)
	n, err := ioWriter.Write(bytesToWrite[:4])
	if err != nil || n != 4 {
		logrus.WithError(err).Error(n)
	}
	ioWriter.Flush()
	return err
}


func (dedupWriter *DedupWriter) FlushData() error {
	dedupWriter.writer.Write(dedupWriter.buffer.Bytes()) //TODO handle error
	logrus.Debugf("Wrote %d Bytes to compressed file", len(dedupWriter.buffer.Bytes()))
	dedupWriter.buffer.Reset()
	dedupWriter.batchCounter = 0
	return nil
}

func (dedupWriter *DedupWriter) FlushAll() error {
	dedupWriter.FlushData()
	dedupWriter.writer.Flush()
	return nil
}
