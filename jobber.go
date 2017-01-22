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

import (
	"log"
	"os"
)

// package logger
var logger *log.Logger

// is you be debugging?
var debug bool

func init() {
	logger = log.New(os.Stderr, "", log.LstdFlags)
}

// SetLogger can be used to define your own logger instance
func SetLogger(l *log.Logger) {
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
