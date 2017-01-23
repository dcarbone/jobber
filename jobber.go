package jobber

/*
	Copyright 2017 Daniel Carbone

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.
*/

// TODO: More better or something?

import (
	"log"
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

// package logger
var logger StdLogger

// is you be debugging?
var debug bool

func init() {
	logger = log.New(os.Stderr, "", log.LstdFlags)
}

// SetLogger can be used to define your own logger instance
func SetLogger(l StdLogger) {
	logger = l
}

// Debug will enable debug-level logging and per-job processing time elapsed calculation
func Debug() {
	debug = true
}

// DisableDebug will disable debug logging and processing time calculation
func DisableDebug() {
	debug = false
}
