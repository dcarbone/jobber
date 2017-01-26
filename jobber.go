package jobber

import "github.com/dcarbone/jobber/log"

// TODO: More better or something?

// is you be debugging?
var debug bool

// SetLogger can be used to define your own logger instance
func SetLogger(l log.StdLogger) {
	log.SetLogger(l)
}

// Debug will enable debug-level logging and per-job processing time elapsed calculation
func Debug() {
	debug = true
}

// DisableDebug will disable debug logging and processing time calculation
func DisableDebug() {
	debug = false
}
