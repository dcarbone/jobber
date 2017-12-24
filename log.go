package jobber

import (
	stdlog "log"
	"os"
)

type Logger interface {
	Print(...interface{})
	Printf(string, ...interface{})
}

var (
	log Logger
)

func init() {
	log = stdlog.New(os.Stdout, "[jobber] ", stdlog.LstdFlags)
}

func SetLogger(l Logger) {
	log = l
}
