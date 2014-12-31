package main

import (
	"io/ioutil"
	"log"
	"os"
	"regexp"
)

var recRe = regexp.MustCompile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}\\|\\|")

func produceRecords(file os.FileInfo, ch chan *LogRecord) {
	bytes, err := ioutil.ReadFile(file.Name())
	if nil != err {
		log.Printf("Error reading %s: %s", file.Name(), err.Error())
		return
	}

	matches := recRe.FindAllIndex(bytes, -1)

	for i := range matches {
		start := matches[i][0]
		end := 0

		if i >= len(matches)-1 {
			end = len(bytes)
		} else {
			end = matches[i+1][0]
		}

		msg := string(bytes[start:end])

		log.Printf("Found message: %s", msg)

		ch <- parseRecord(msg)
	}
}
