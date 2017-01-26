package log

import (
	stdLog "log"
	"os"
)

type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})

	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Fatalln(v ...interface{})

	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
	Panicln(v ...interface{})
}

const logPrefixStr = "[jobber] "

var logPrefixSlice []interface{}

// package logger
var logger StdLogger

var custom bool

func init() {
	logger = stdLog.New(os.Stderr, logPrefixStr, stdLog.LstdFlags)
	logPrefixSlice = []interface{}{logPrefixStr}
}

func SetLogger(l StdLogger) {
	custom = true
	logger = l
}

func Print(v ...interface{}) {
	if custom {
		logger.Print(append(logPrefixSlice, v...)...)
	} else {
		logger.Print(v...)
	}
}
func Printf(format string, v ...interface{}) {
	if custom {
		logger.Printf(logPrefixStr+format, v...)
	} else {
		logger.Printf(format, v...)
	}
}
func Println(v ...interface{}) {
	if custom {
		logger.Println(append(logPrefixSlice, v...)...)
	} else {
		logger.Println(v...)
	}
}

func Fatal(v ...interface{}) {
	if custom {
		logger.Fatal(append(logPrefixSlice, v...)...)
	} else {
		logger.Fatal(v...)
	}
}
func Fatalf(format string, v ...interface{}) {
	if custom {
		logger.Fatalf(logPrefixStr+format, v...)
	} else {
		logger.Fatalf(format, v...)
	}
}
func Fatalln(v ...interface{}) {
	if custom {
		logger.Fatalln(append(logPrefixSlice, v...)...)
	} else {
		logger.Fatalln(v...)
	}
}

func Panic(v ...interface{}) {
	if custom {
		logger.Panic(append(logPrefixSlice, v...)...)
	} else {
		logger.Panic(v...)
	}
}
func Panicf(format string, v ...interface{}) {
	if custom {
		logger.Panicf(logPrefixStr+format, v...)
	} else {
		logger.Panicf(format, v...)
	}
}
func Panicln(v ...interface{}) {
	if custom {
		logger.Panicln(append(logPrefixSlice, v...)...)
	} else {
		logger.Panicln(v...)
	}
}
