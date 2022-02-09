package core

import (
	"Deduper/IO"
	"Deduper/config"
	"Deduper/crypto"
	"bufio"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

// data for the algorithm
var offsetsArr = make([]int, 0)
var startsSet = make(map[string]struct{})
var hashToOffset = make(map[string]int)

// for debugging
var offsetToLength = make(map[int]int)

func Dedup(inputFilePath, outputFilePath *string) error{
	// init file reader
	file, reader, err := IO.InitDedupFileReader(inputFilePath)
	if err != nil {
		logrus.WithError(err)
		return err
	}
	defer IO.CloseFile(file)
	// init file writer
	dedupWriter, err := IO.NewDedupWriter(outputFilePath, config.MaxChunksInWriterBuffer, config.MaxChunkSizeInBytes)
	defer dedupWriter.Close()
	err = dedupe(reader, dedupWriter)
	if err != nil {
		logrus.WithError(err)
		return err
	}
	dedupWriter.FlushAll()
	//writeDebugMetaData(outputFilePath)
	return err
}

func dedupe(reader *bufio.Reader, writer *IO.DedupWriter) error {
	var err error
	var newBytes *[]byte
	// write blank 4 bytes - will be used for the offset of the metadata
	padding := make([]byte, 4)
	n, err := writer.WriteBlank(&padding)
	if err != nil || n != 4 {
		logrus.WithError(err).Errorf("Could not write blank 4 bytes")
		return err
	}

	// chunk the data
	buffer := make([]byte, 0)
	for {
		if err != nil {
			break
		}
		if writer.CurrentOffset == 18132792 {
			println("here")
		}
		if len(buffer) < 2 * config.MaxChunkSizeInBytes {
			newBytes, err = getBytes(reader)
			if err != nil {
				break
			}
			buffer = append(buffer, (*newBytes)[:]...)
		}
		cutPoint, err := chunk(&buffer, writer)
		if err != nil {
			logrus.WithError(err)
			return err
		}
		buffer = buffer[cutPoint:]
	}
	// chunk the EOF (need special care because we can't allow chunks in less than minChunkSize)
	if err == io.EOF {
		err = chunkEOF(&buffer, writer) // maxChunkSizeInBytes <= size of buffer < 2 maxChunkSizeInBytes
	}
	if err != nil {
		logrus.WithError(err)
		return err
	}
	metadataOffset := writer.CurrentOffset

	// write metadata
	n, err = writer.WriteMataData(offsetsArr)
	if err != nil {
		logrus.WithError(err).Errorf("Error - WriteMataData")
	}
	writer.FlushData()

	writer.WriteMataDataOffset(metadataOffset)

	return err
}


func getBytes(reader *bufio.Reader) (*[]byte, error) {
	logrus.Debugf("getBytes called\n")
	buf := make([]byte, 0, config.ReadBufferSizeInBytes)
	n, err := reader.Read(buf[:cap(buf)])
	buf = buf[:n]
	if n == 0 {
		if err == nil || err == io.EOF {
			return nil, io.EOF
		}
		return nil, err
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	return &buf, nil
}


// chunk
// returns the index in the buffer which the new buffer should begin from
func chunk(buffer *[]byte, writer *IO.DedupWriter) (int, error) {
	cutPoint := config.MinChunkSizeInBytes
	for {
		if cutPoint > config.MaxChunkSizeInBytes || cutPoint >= len(*buffer) { // we got to the MaxChunkSize
			data := (*buffer)[:config.MinChunkSizeInBytes]
			newChunkId := getCreateChunk(&data, writer)
			addChunkToFile(newChunkId)
			logrus.Debugf("cutPoint : %d\n", config.MinChunkSizeInBytes)
			return config.MinChunkSizeInBytes, nil
		}
		exists, id := getChunk((*buffer)[cutPoint-config.MinChunkSizeInBytes: cutPoint])
		if !exists {
			cutPoint+=1
			continue
		}
		// check if we can split the buffer into 2 or more chunks, or we should insert new chunk
		if cutPoint-(2*config.MinChunkSizeInBytes) < 0 { // we should insert new chunk for the whole buffer until cutPoint
			data := (*buffer)[:cutPoint]
			newChunkId := getCreateChunk(&data, writer)
			addChunkToFile(newChunkId)
		} else { // we should split the buffer into 2 or more chunks
			prefix := (*buffer)[:cutPoint-config.MinChunkSizeInBytes]
			prefixChunkId := getCreateChunk(&prefix, writer)
			addChunkToFile(prefixChunkId)
			addChunkToFile(id)
		}
		logrus.Debugf("cutPoint : %d\n",cutPoint)
		return cutPoint, nil
	}
}

func chunkEOF(buffer *[]byte, writer *IO.DedupWriter) error {
	logrus.Debugf("chunkEOF %s", *buffer)
	startCutPoint := 0
	endCutPoint := config.MinChunkSizeInBytes
	var chunkId int
	var data []byte
	for {
		if len(*buffer) - endCutPoint < config.MinChunkSizeInBytes {
			break
		}
		data = (*buffer)[startCutPoint:endCutPoint]
		chunkId = getCreateChunk(&data, writer)
		addChunkToFile(chunkId)
		startCutPoint = endCutPoint
		endCutPoint += config.MinChunkSizeInBytes
	}
	eof := (*buffer)[startCutPoint:]
	chunkId = getCreateChunk(&eof, writer)
	addChunkToFile(chunkId)
	return nil
}

// getCreateChunk
// returns the chunkId of the data
func getCreateChunk(data *[]byte, writer *IO.DedupWriter) int {
	var offest int
	exists, existingChunkId := getChunk(*data)
	if exists {
		offest = existingChunkId
	} else {
		offest = createNewChunk(data, writer)
	}
	return offest
}

// getChunk
// return false if there is no existing chunk for the data.
// o.w true and the offset of the chunk
func getChunk(data []byte) (bool, int){ //TODO switch to reference
	_, okStart := startsSet[string(data[:config.StartLength])] // it is possible to create bloom filter like with different offsets
	if !okStart {
		return false, 0
	}
	hash := crypto.Checksum(data)
	val, ok := hashToOffset[hash]
	return ok, val
}

// createNewChunk
// return the chunk offset of the data
func createNewChunk (data *[]byte, writer *IO.DedupWriter) int  {
	startsSet[string((*data)[:config.StartLength])] = struct{}{} // for faster performance
	hash := crypto.Checksum(*data)
	offset := writer.CurrentOffset
	hashToOffset[hash] = offset
	//offsetToLength[offset] = len(*data)
	n, err := writer.WriteData(data)
	if err != nil {
		logrus.Debugf("Error WriteString")
	}
	logrus.Debugf("hashToOffset[%s] = %d | data length - %d (%d)\n", hash, offset, len(*data), len(*data)+4)
	writer.CurrentOffset += n
	return offset
}

func addChunkToFile(offest int){
	logrus.Debugf("addChunkToFile ------> %d", offest)
	offsetsArr = append(offsetsArr, offest)
}

func writeDebugMetaData(filePath *string){
	f, err := os.Create(*filePath + "metadata.txt")
	if err != nil {
		logrus.WithError(err)
	}
	writer := bufio.NewWriter(f)
	currentOffset := 0
	for i := 0; i < len(offsetsArr) ; i++ {
		offset := offsetsArr[i]
		str := fmt.Sprintf("Offset:%d | dataOffset:%d, length:%d \n", currentOffset, offset, offsetToLength[offset])
		currentOffset += offsetToLength[offset]
		writer.Write([]byte(str))
	}
	writer.Flush()
	f.Close()
}
