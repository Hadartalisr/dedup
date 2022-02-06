package core

import (
	"Deduper/IO"
	"Deduper/config"
	"bufio"
	"encoding/binary"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"time"
)

func UnDedup(inputFilePath, outputFilePath *string) error{
	undedupStartTime := time.Now()

	offsetsArray, _ := getOffsetsArray(inputFilePath)
	undedupDataReader, err := IO.NewUndedupFileReader(inputFilePath, config.MaxChunkSizeInBytes)
	UndedupWriter, err := IO.NewUnDedupWriter(outputFilePath, config.MaxChunksInWriterBuffer, config.MaxChunkSizeInBytes)

	for _, offset := range *offsetsArray {
		data, _ := undedupDataReader.GetChunk(offset) //TODO handle error
		UndedupWriter.WriteData(data)
	}
	undedupDataReader.Close()
	UndedupWriter.Close()
	elapsedTime := time.Now().Sub(undedupStartTime).Seconds()
	logrus.Infof("UnDedup time - %f seconds." , elapsedTime)
	return err
}

func getOffsetsArray(outputFilePath *string) (*[]int, error){
	outputFile, err := os.Open(*outputFilePath)
	defer outputFile.Close()
	if err != nil {
		return nil, err
	}
	metadataOffset := getMetadataOffset(outputFile)
	f, err := os.Open(*outputFilePath)
	defer f.Close()
	offsetsArr := generateOffsetArray(f, metadataOffset)
	return offsetsArr, nil
}

func getMetadataOffset(outputFile *os.File) int{
	outputFile.Seek(0, io.SeekStart)
	reader := bufio.NewReader(outputFile)
	metadataOffset := getIntFromReader(reader)
	return metadataOffset
}

func generateOffsetArray(outputFile *os.File, metadataOffset int) *[]int {
	outputFile.Seek(int64(metadataOffset), io.SeekStart)
	metadataReader := bufio.NewReader(outputFile)
	metadataBytes, err :=  ioutil.ReadAll(metadataReader)
	if err != nil {
		logrus.WithError(err).Errorf("ERROR")
	}
	index := 0
	metaDataLength := binary.LittleEndian.Uint32(metadataBytes[index: index+4])
	offsetsArr := make([]int, 0)
	for {
		index++
		if index > int(metaDataLength){
			break
		}
		offset := binary.LittleEndian.Uint32(metadataBytes[index*4: (index+1)*4])
		offsetsArr = append(offsetsArr, int(offset))
	}
	return &offsetsArr
}

func getIntFromReader(reader *bufio.Reader) int {
	arr := make([]byte, 0)
	for i:=0 ; i<4 ; i++ {
		byte , err := reader.ReadByte() //TODO handle error
		if err != nil {
			logrus.WithError(err).Errorf("Error - getIntFromReader")
		}
		arr = append(arr, byte)
	}
	num := binary.LittleEndian.Uint32(arr[:4])
	return int(num)
}
