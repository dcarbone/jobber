package jobber

import (
	"fmt"
	stdlog "log"
	"os"
)

const logPrefixStr = "[jobber] "

type (
	Job interface {
		RespondTo() chan error
		Process() error
	}
	Logger interface {
		Print(v ...interface{})
		Printf(format string, v ...interface{})
		Println(v ...interface{})
	}
	logWrap struct{}
)

var (
	logPrefixSlice = []interface{}{logPrefixStr}
	log            *logWrap
	logger         Logger = stdlog.New(os.Stderr, "", stdlog.LstdFlags)
	debug          bool
)

func SetLogger(l Logger) {
	logger = l
}

func Debug() {
	debug = true
}

func DisableDebug() {
	debug = false
}

func (l *logWrap) Printf(format string, v ...interface{}) {
	logger.Printf(fmt.Sprintf("%s%s", logPrefixStr, format), v...)
}
func (l *logWrap) Print(v ...interface{}) {
	logger.Print(append(logPrefixSlice, v...)...)
}
func (l *logWrap) Println(v ...interface{}) {
	logger.Println(append(logPrefixSlice, v...)...)
}
