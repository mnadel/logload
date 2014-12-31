package main

import (
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	Debug   = 0
	Info    = 1
	Warn    = 2
	Error   = 3
	Fatal   = 4
	Unknown = 10
)

type LogLevel int

type LogRecord struct {
	Timestamp time.Time
	Level     LogLevel
	Message   string
	Logger    string
	User      string
	Pid       int16
	Tid       int16
}

func parseInt(formatted string) int16 {
	parsed, err := strconv.ParseInt(formatted, 10, 16)
	if err != nil {
		log.Fatalf("Error parsing int %s: %s", formatted, err.Error())
	}

	return int16(parsed)
}

func parseTimestamp(formatted string) time.Time {
	parsed, err := time.Parse("2006-01-02 15:04:05.999999999", formatted)
	if err != nil {
		log.Fatalf("Error parsing time %s: %s", formatted, err.Error())
	}

	return parsed
}

func parseLevel(level string) LogLevel {
	switch {
	case "DEBUG" == level:
		return Debug
	case "INFO" == level:
		return Info
	case "WARN" == level:
		return Warn
	case "ERROR" == level:
		return Error
	case "FATAL" == level:
		return Fatal
	}

	return Unknown
}

var re = regexp.MustCompile("\\|\\|")

func parseRecord(message string) *LogRecord {
	parts := re.Split(message, -1)

	return &LogRecord{
		Timestamp: parseTimestamp(parts[0]),
		User:      parts[1],
		Pid:       parseInt(parts[2]),
		Tid:       parseInt(parts[3]),
		Level:     parseLevel(parts[4]),
		Logger:    parts[5],
		Message:   strings.Trim(parts[6], "\n"),
	}
}
