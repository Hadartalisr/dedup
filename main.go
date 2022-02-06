package main

import (
	"Deduper/IO"
	"Deduper/config"
	"Deduper/crypto"
	"Deduper/test"
	"bufio"
	"encoding/binary"
	"flag"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"time"
)

// vars for users arguments
var inputFilePath string = ""
var outputFilePath string = ""

// deduplication performance
var (
	startTime time.Time
	chunkfound = 0
	chunkNotFound = 0
	inputFileSize = 0
	outputFileSize = 0
)

// data for the algorithm
var offsetsArr []int = make([]int, 0)
var startsSet = make(map[string]struct{})
var hashToOffset = make(map[uint32]int)


func main() {
	// set debug level
	logrus.SetLevel(config.LogLevel)
	// get user input
	getArgs()
	shouldDedup := flag.Bool("dedup", false, "indicated if we should dedup")
	shouldUndedup := flag.Bool("undedup", false, "indicated if we should undedup")
	shouldCompare := flag.Bool("compare", false, "indicated if we should compare")
	flag.Parse()
	startTime = time.Now()
	if *shouldDedup {
		Dedup()
		info("dedup")
	} else if *shouldUndedup {
		UnDedup()
		info("undedup")
	} else if *shouldCompare {
		Test()
		info("compare")
	} else {
		logrus.Error("ERROR - nor dedup/undedup/compare flag was passed")
		os.Exit(1)
	}

}

func getArgs() {
	args := os.Args
	if len(args) < 4 {
		logrus.Errorf("ERROR - Not enough arguments have been received. expected - 2, got - %d", len(args))
		os.Exit(1)
	}

	for i:=1 ; i < len(args) ; i++ {
		if args[i][0] == '-' {
			continue
		}
		if inputFilePath == "" {
			inputFilePath = args[i]
			continue
		}
		if outputFilePath == "" {
			outputFilePath = args[i]
			break
		}
	}
}











func Dedup() error{
	// init file reader
	file, reader, err := IO.InitDedupFileReader(inputFilePath)
	if err != nil {
		logrus.Debugf("Error occured during InitDedupFileReader")
		print(err)
	}
	defer IO.CloseFile(file)


	dedupWriter, err := IO.NewDedupWriter(outputFilePath, config.MaxChunksInWriterBuffer, config.MaxChunkSizeInBytes)
	defer dedupWriter.Close()

	err = dedup(reader, dedupWriter)
	if err != nil {
		logrus.Debugf("Error occured during core")
		print(err)
	}
	dedupWriter.FlushAll()
	return err
}

func dedup(reader *bufio.Reader, writer *IO.DedupWriter) error {
	var err error
	var newBytes *[]byte
	// write byte for later - will be used for the offset of the metadata
	padding := make([]byte, 4)
	n, err := writer.WriteBlank(&padding)
	if err != nil || n != 4 {
		logrus.WithError(err).Errorf("Could not write blank 4 bytes")
	}

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
	}
	if err != nil {
		logrus.WithError(err).Errorf("Error")
	}
	metadataOffset := writer.CurrentOffset

	// write metadata
	n, err = writer.WriteMataData(offsetsArr)
	if err != nil {
		logrus.WithError(err).Errorf("Error - WriteMataData")
	}
	writer.FlushData()

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
	startsSet[string((*data)[:config.StartLength])] = struct{}{} // for faster performance
	hash := crypto.Checksum(*data)
	offset := writer.CurrentOffset
	hashToOffset[hash] = offset
	n, err := writer.WriteData(data) //TODO writer to file in a buffer
	if err != nil {
		logrus.Debugf("Error WriteString") //TODO handle
	}
	logrus.Debugf("hashToOffset[%d] = %d | data length - %d (%d)\n", hash, offset, len(*data), len(*data)+4)
	writer.CurrentOffset += n
	return offset
}

func addChunkToFile(offest int){
	logrus.Debugf("addChunkToFile ------> %d", offest)
	offsetsArr = append(offsetsArr, offest)
}

// info
// action in ["dedup", "undedup","compare"]
func info(action string) {
	inputFile, err := os.Open(inputFilePath)
	outputFile, err := os.Open(outputFilePath)

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


	logrus.Infof("Process time - %f seconds." , elapsedTime)
	logrus.Infof("Process speed - %f MB/Sec", float64(inputFileSizeInMB)/elapsedTime)
	logrus.Infof("Input File size - %d Bytes", inputFileSize)
	logrus.Infof("Output File size - %d Bytes", outputFileSize)

	if action == "dedup" {
		logrus.Infof("Dedup - chunks FOUNT - %d Chunks NOT FOUNT - %d\n", chunkfound, chunkNotFound)
		logrus.Infof("Dedup factor - %f", float64(inputFileSize)/float64(outputFileSize))
	}
}













func UnDedup() error{
	undedupStartTime := time.Now()

	offsetsArray, _ := getOffsetsArray(&inputFilePath)
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







func Test () {
	_, err := test.Equal(inputFilePath, outputFilePath)
	if err != nil {
		logrus.Debugf("Error occured Equality test")
		print(err)
	}
}



