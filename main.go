package main

import (
	"bufio"
	"deduplication/IO"
	"deduplication/config"
	"deduplication/crypto"
	"deduplication/test"
	"encoding/binary"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

var filename = "5000-100"
var fileSuffix = "" //".txt"
var inputFilePath = filepath.Join(config.InputDirectoryPath, filename + fileSuffix )
var outputFilePath = filepath.Join(config.OutputDirectoryPath, filename + "-compressed" + fileSuffix)
var undedupOutputFilePath = filepath.Join(config.OutputDirectoryPath,  filename + fileSuffix)

// deduplication performance
var (
	startTime time.Time
	chunkfound = 0
	chunkNotFound = 0
	inputFileSize = 0
	outputFileSize = 0
)


// data for the algorithm
var startsSet = make(map[string]struct{})
var hashToOffset = make(map[uint32]int)

// tmp vars
var offsetsArr []int = make([]int, 0)

func info(inputFile , outputFile *os.File) {
	elapsedTime := time.Now().Sub(startTime).Seconds()
	fileInfo, err := inputFile.Stat()
	if err != nil {
		// TODO handle
	}
	inputFileSize := fileInfo.Size()
	fileInfo, err = outputFile.Stat()
	if err != nil {
		// TODO handle
	}
	outputFileSize := fileInfo.Size()

	inputFileSizeInMB := inputFileSize / (1024 * 1024)


	logrus.Infof("Dedup time - %f seconds." , elapsedTime)
	logrus.Infof("Dedup speed - %f MB/Sec", float64(inputFileSizeInMB)/elapsedTime)
	logrus.Infof("Chunks FOUNT - %d Chunks NOT FOUNT - %d\n", chunkfound, chunkNotFound)
	logrus.Infof("Input File size - %d Bytes", inputFileSize)
	logrus.Infof("Output File size - %d Bytes", outputFileSize)
	logrus.Infof("Dedup factor - %f", float64(inputFileSize)/float64(outputFileSize))
}

func main() {
	Dedup()
	UnDedup()
	Test()
}

func Test () {
	_, err := test.Equal(inputFilePath,   undedupOutputFilePath)
	if err != nil {
		logrus.Debugf("Error occured Equality test")
		print(err)
	}
}

func UnDedup() error{
	undedupStartTime := time.Now()

	offsetsArray, _ := getOffsetsArray(&outputFilePath)
	undedupDataReader, err := IO.NewUndedupFileReader(outputFilePath, config.MaxChunkSizeInBytes)
	UndedupWriter, err := IO.NewUnDedupWriter(undedupOutputFilePath, config.MaxChunksInWriterBuffer, config.MaxChunkSizeInBytes)

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
	reader := bufio.NewReader(outputFile)
	buf := make([]byte, 4)
	n, err := io.ReadAtLeast(reader, buf,4)
	if err != nil || n < 4 {
		//TODO error
	}
	metadataOffset := binary.LittleEndian.Uint32(buf)
	outputFile.Seek(int64(metadataOffset),0)
	metadataReader := bufio.NewReader(outputFile)
	metadataBytes, err :=  ioutil.ReadAll(metadataReader)
	if err != nil || n < 4 {
		//TODO error
	}
	index := 0
	offsetsArr := make([]int, 0)
	for {
		if index == len(metadataBytes){
			break
		}
		offset := binary.LittleEndian.Uint32(metadataBytes[index: index+4])
		offsetsArr = append(offsetsArr, int(offset))
		index+=4
	}
	return &offsetsArr, nil
}


func Dedup() error{
	initDedupe()

	// init file reader
	file, reader, err := IO.InitDedupFileReader(inputFilePath)

	if err != nil {
		logrus.Debugf("Error occured during InitDedupFileReader")
		print(err)
	}
	defer IO.CloseFile(file)


	dedupWriter, err := IO.NewDedupWriter(outputFilePath, config.MaxChunksInWriterBuffer, config.MaxChunkSizeInBytes)
	startTime = time.Now()

	err = dedup(reader, dedupWriter)
	if err != nil {
		logrus.Debugf("Error occured during core")
		print(err)
	}
	defer dedupWriter.Close()
	dedupWriter.FlushAll()




	info(file, dedupWriter.OutputFile)
	return nil
}

func initDedupe() {
	logrus.SetLevel(config.LogLevel)
}


func dedup(reader *bufio.Reader, writer *IO.DedupWriter) error {
	var err error
	var newBytes *[]byte
	// write byte for later - will be used for the offset of the metadata
	padding := make([]byte, 1)
	writer.WriteData(&padding)

	// write data
	buffer := make([]byte, 0)
	for {
		if err != nil {
			break
		}
		if len(buffer) < 2 * config.MaxChunkSizeInBytes { //TODO switch all strings to work with bytes
			newBytes, err = getBytes(reader)
			if err != nil {
				break
			}
			buffer = append(buffer, (*newBytes)[:]...)
		}
		index, _ := chunk(&buffer, writer)
		buffer = buffer[index:]
	}
	if err == io.EOF {
		err = chunkEOF(&buffer, writer) // maxChunkSizeInBytes <= size of buffer < 2 maxChunkSizeInBytes
		return nil
	}
	metadataOffset := writer.CurrentOffset
	// write metadata
	writer.WriteMataData(offsetsArr)

	// write metadata offset
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
		if cutPoint > config.MaxChunkSizeInBytes || cutPoint >= len(*buffer) {
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
		chunkfound++
		offest = existingChunkId
	} else {
		chunkNotFound++
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
	startsSet[string((*data)[:config.StartLength])] = struct{}{}
	hash := crypto.Checksum(*data)
	offset := writer.CurrentOffset
	n, err := writer.WriteData(data) //TODO writer to file in a buffer
	if err != nil {
		logrus.Debugf("Error WriteString") //TODO handle
	}
	hashToOffset[hash] = offset
	writer.CurrentOffset += n
	logrus.Debugf("\ncreateNewChunk | hashToOffset[%d] = %s \n", hash, offset)
	return offset
}

func addChunkToFile(offest int){
	logrus.Debugf("addChunkToFile ------> %d", offest)
	offsetsArr = append(offsetsArr, offest)
}










