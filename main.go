package main

import (
	"bufio"
	"deduplication/IO"
	"deduplication/crypto"
	"deduplication/test"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"path/filepath"
	"time"
)

var filename = "5000-100"
var fileSuffix = "" //".txt"
var inputFilePath = filepath.Join(inputDirectoryPath, filename + fileSuffix )
var outputFilePath = filepath.Join(outputDirectoryPath, filename + "-compressed" + fileSuffix)
var undedupOutputFilePath = filepath.Join(outputDirectoryPath,  filename + fileSuffix)

// deduplication performance
var (
	startTime time.Time
	chunkfound = 0
	chunkNotFound = 0
	inputFileSize = 0
	outputFileSize = 0
)

// const variables
const (
	logLevel = logrus.InfoLevel
	inputDirectoryPath = "input"
	outputDirectoryPath = "output"
	startLength = 20
	minChunkSizeInBytes   = 8 * 1024
	maxChunkSizeInBytes   = 32 * 1024
	maxChunksInWriterBuffer   = 3000
	readBufferSizeInBytes = maxChunkSizeInBytes * 100
)


// data for the algorithm
var startsSet = make(map[string]struct{})
var hashToOffset = make(map[uint32]int)

// tmp vars
var hashFile []int = make([]int, 0)

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
	undedupReader, err := IO.NewUndedupFileReader(outputFilePath, maxChunkSizeInBytes)
	UndedupWriter, err := IO.NewUnDedupWriter(undedupOutputFilePath, maxChunksInWriterBuffer, maxChunkSizeInBytes)
	for _, offset := range hashFile {
		data, _ := undedupReader.GetChunk(offset) //TODO handle error
		UndedupWriter.WriteData(data)
	}
	undedupReader.Close()
	UndedupWriter.Close()
	elapsedTime := time.Now().Sub(undedupStartTime).Seconds()
	logrus.Infof("UnDedup time - %f seconds." , elapsedTime)
	return err
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


	dedupWriter, err := IO.NewDedupWriter(outputFilePath, maxChunksInWriterBuffer,maxChunkSizeInBytes)
	startTime = time.Now()

	err = dedup(reader, dedupWriter)
	if err != nil {
		logrus.Debugf("Error occured during dedup")
		print(err)
	}
	defer dedupWriter.Close()
	dedupWriter.FlushAll()




	info(file, dedupWriter.OutputFile)
	return nil
}

func initDedupe() {
	logrus.SetLevel(logLevel)
}


func dedup(reader *bufio.Reader, writer *IO.DedupWriter) error {
	var err error
	var newBytes *[]byte
	buffer := make([]byte, 0)
	for {
		if err != nil {
			break
		}
		if len(buffer) < 2 *maxChunkSizeInBytes { //TODO switch all strings to work with bytes
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
	return  err
}


func getBytes(reader *bufio.Reader) (*[]byte, error) {
	logrus.Debugf("getBytes called\n")
	buf := make([]byte, 0, readBufferSizeInBytes)
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
	cutPoint := minChunkSizeInBytes
	for {
		if cutPoint > maxChunkSizeInBytes || cutPoint >= len(*buffer) {
			data := (*buffer)[:minChunkSizeInBytes]
			newChunkId := getCreateChunk(&data, writer)
			addChunkToFile(newChunkId)
			logrus.Debugf("cutPoint : %d\n", minChunkSizeInBytes)
			return minChunkSizeInBytes, nil
		}
		exists, id := getChunk((*buffer)[cutPoint-minChunkSizeInBytes: cutPoint])
		if !exists {
			cutPoint+=1
			continue
		}
		// check if we can split the buffer into 2 or more chunks, or we should insert new chunk
		if cutPoint-(2*minChunkSizeInBytes) < 0 { // we should insert new chunk for the whole buffer until cutPoint
			data := (*buffer)[:cutPoint]
			newChunkId := getCreateChunk(&data, writer)
			addChunkToFile(newChunkId)
		} else { // we should split the buffer into 2 or more chunks
			prefix := (*buffer)[:cutPoint-minChunkSizeInBytes]
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
	endCutPoint := minChunkSizeInBytes
	var chunkId int
	var data []byte
	for {
		if len(*buffer) - endCutPoint < minChunkSizeInBytes {
			break
		}
		data = (*buffer)[startCutPoint:endCutPoint]
		chunkId = getCreateChunk(&data, writer)
		addChunkToFile(chunkId)
		startCutPoint = endCutPoint
		endCutPoint += minChunkSizeInBytes
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
	_, okStart := startsSet[string(data[:startLength])] // it is possible to create bloom filter like with different offsets
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
	startsSet[string((*data)[:startLength])] = struct{}{}
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
	hashFile = append(hashFile, offest)
}










