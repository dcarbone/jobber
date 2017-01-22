package jobber

import (
	"sync"
	"fmt"
	"log"
)

var boss sync.RWMutex

var workers map[string]*worker

var logger *log.Logger

var debug bool

func init() {
	boss = sync.RWMutex{}
	workers = make(map[string]*worker)

	logger = &log.Logger{}
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

// HasWorker lets you know if a worker already exists for a job
func HasWorker(name string) bool {
	boss.RLock()
	defer boss.RUnlock()

	if _, ok := workers[name]; ok {
		return true
	}

	return false
}

// NewWorker will attempt to create a new worker for a job. Returns error if you've already hired somebody.
func NewWorker(name string, queueLength int) error {
	// see if we're already got somebody doin' it
	if HasWorker(name) {
		if debug {
			logger.Printf("Jobber: A worker for job \"%s\" already exists.\n", name)
		}
		return fmt.Errorf("A worker for job \"%s\" already exists.", name)
	}

	// 'lil input sanitizing
	if 0 > queueLength {
		// shout
		if debug {
			logger.Printf(
				"Jobber: Incoming new worker request for job \"%s\" request specified invalid queue" +
					" length of \"%d\", will set length to \"0\"\n",
				name,
				queueLength)
		}
		// make unbuffered
		queueLength = 0
	}

	// tell the world
	logger.Printf(
		"Jobber: Creating new worker for job with name \"%s\" and queue length of \"%d\"\n",
		name,
		queueLength)

	// lock boss down
	boss.Lock()
	defer boss.Unlock()

	// add worker
	workers[name] = &worker{
		name: name,
		jobs: make(chan Job, queueLength),
	}

	// start up work routine
	go workers[name].doWork()

	// maybe say so?
	if debug {
		logger.Printf("Jobber: Go routine started for job \"%s\"\n", name)
	}

	return nil
}

// AddWork will push a new job to the end of a worker's queue
func AddJob(workerName string, j Job) error {
	// see if we've already hired this worker
	if false == HasWorker(workerName) {
		if debug {
			logger.Printf("Jobber: No worker with \"%s\" found.\n", workerName)
		}
		return fmt.Errorf("No worker with name \"%s\" found", workerName)
	}

	// lock boss down
	boss.RLock()
	defer boss.RUnlock()

	// add to worker queue
	return workers[workerName].addJob(j)
}

// NewUnbufferedWorker will attempt to create a new worker with a queue length of 0
func NewUnbufferedWorker(name string) error {
	return NewWorker(name, 0)
}
