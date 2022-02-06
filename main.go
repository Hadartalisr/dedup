package main

import (
	"Deduper/config"
	"Deduper/core"
	"flag"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)


func main() {
	// set debug level
	logrus.SetLevel(config.LogLevel)
	// get user input
	inputFilePath, outputFilePath := getArgs()
	shouldDedup := flag.Bool("dedup", false, "indicated if we should dedup")
	shouldUndedup := flag.Bool("undedup", false, "indicated if we should undedup")
	shouldCompare := flag.Bool("compare", false, "indicated if we should compare")
	flag.Parse()
	startTime := time.Now()
	if *shouldDedup {
		core.Dedup(inputFilePath, outputFilePath)
		info(startTime, inputFilePath, outputFilePath, "dedup")
	} else if *shouldUndedup {
		core.UnDedup(inputFilePath, outputFilePath)
		info(startTime, inputFilePath, outputFilePath, "undedup")
	} else if *shouldCompare {
		core.Compare(inputFilePath, outputFilePath)
		info(startTime, inputFilePath, outputFilePath, "compare")
	} else {
		logrus.Error("ERROR - nor dedup/undedup/compare flag was passed")
		os.Exit(1)
	}

}

func getArgs() (*string, *string) {
	args := os.Args
	if len(args) < 4 {
		logrus.Errorf("ERROR - Not enough arguments have been received. expected - 2, got - %d", len(args))
		os.Exit(1)
	}
	var inputFilePath string
	var outputFilePath string
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
	return &inputFilePath, &outputFilePath
}


// info
// action in ["dedup", "undedup","compare"]
func info(startTime time.Time, inputFilePath, outputFilePath *string, action string) {
	inputFile, err := os.Open(*inputFilePath) //TODO handle errors
	outputFile, err := os.Open(*outputFilePath)

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
	logrus.Infof("Input File [%s] size - %d Bytes", *inputFilePath, inputFileSize)
	logrus.Infof("Output File [%s] size - %d Bytes", *outputFilePath, outputFileSize)
	if action == "dedup" {
		logrus.Infof("Dedup factor - %f", float64(inputFileSize)/float64(outputFileSize))
	}
}






















