package main

import (
	"Deduper/config"
	"Deduper/core"
	"flag"
	"github.com/sirupsen/logrus"
	"os"
	"runtime"
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
	inputFile, err := os.Open(*inputFilePath)
	if err != nil {
		logrus.WithError(err)
		return
	}
	outputFile, err := os.Open(*outputFilePath)
	if err != nil {
		logrus.WithError(err)
		return
	}
	elapsedTime := time.Now().Sub(startTime).Seconds()
	fileInfo, err := inputFile.Stat()
	if err != nil {
		logrus.WithError(err)
		return
	}
	inputFileSize := fileInfo.Size()
	fileInfo, err = outputFile.Stat()
	if err != nil {
		logrus.WithError(err)
		return
	}
	outputFileSize := fileInfo.Size()
	inputFileSizeInMB := bToMb(uint64(inputFileSize))
	outputFileSizeInMB := bToMb(uint64(outputFileSize))
	logrus.Infof("Process time - %f seconds." , elapsedTime)
	logrus.Infof("Input File [%s] size - %d Bytes", *inputFilePath, inputFileSize)
	logrus.Infof("Output File [%s] size - %d Bytes", *outputFilePath, outputFileSize)
	if action == "dedup" {
		logrus.Infof("Dedup factor - %f", float64(inputFileSize)/float64(outputFileSize))
		logrus.Infof("Process speed - %f MB/Sec", float64(inputFileSizeInMB)/elapsedTime)
		PrintMemUsage()
	}
	if action == "undedup" {
		logrus.Infof("Process speed - %f MB/Sec", float64(outputFileSizeInMB)/elapsedTime)
		PrintMemUsage()
	}
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	logrus.Info("***** Memory Usage *****")
	logrus.Infof("\tAlloc = %d MiB", bToMb(m.Alloc))
	logrus.Infof("\tSys = %v MiB", bToMb(m.Sys))
	logrus.Infof("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return uint64(float64(b) / 1024 / 1024)
}





















