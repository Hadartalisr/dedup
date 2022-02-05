package IO

import (
	"bufio"
	"bytes"
	"os"
)

type UnDedupWriter struct {
	outputFile *os.File
	batchCounter int
	maxBatch int
	buffer *bytes.Buffer
	writer *bufio.Writer
}

func NewUnDedupWriter(filePath string, chunkMaxSize, maxChunksInBatch int) (*UnDedupWriter, error) {
	outputFile, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriter(outputFile)
	buf := &bytes.Buffer{}
	buf.Grow(chunkMaxSize * maxChunksInBatch)
	dedupWriter := UnDedupWriter{
		outputFile,
		0,
		maxChunksInBatch,
		buf,
		writer,
	}
	return &dedupWriter, err
}

func (writer *UnDedupWriter) Close()  error {
	writer.FlushData()
	writer.FlushAll()
	writer.outputFile.Close()
	return nil
}

// WriteData
// return the number of bytes which were written
func (writer *UnDedupWriter) WriteData(data *[]byte) (int, error) {
	if writer.batchCounter > writer.maxBatch {
		writer.FlushData()
	}
	writer.batchCounter++
	writer.buffer.Write(*data)
	return len(*data), nil
}

func (undedupWriter *UnDedupWriter) FlushData() error {
	undedupWriter.writer.Write(undedupWriter.buffer.Bytes()) //TODO handle error
	//logrus.Infof("Wrote %d Bytes from compressed file", len(undedupWriter.buffer.Bytes()))
	undedupWriter.buffer.Reset()
	undedupWriter.batchCounter = 0
	return nil
}

func (undedupWriter *UnDedupWriter) FlushAll() error {
	undedupWriter.writer.Flush()
	return nil
}

