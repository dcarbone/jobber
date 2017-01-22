package jobber

import (
	"sync"
	"fmt"
	"log"
)

// package logger
var logger *log.Logger

// is you be debugging?
var debug bool

// Boss controls the life of the worker
type Boss struct {
	*sync.RWMutex

	workers map[string]*worker
	hr      chan *worker
}

func init() {
	logger = &log.Logger{}
}

// New creates a Boss
func New(name string) *Boss {
	// initialize boss
	b := &Boss{
		RWMutex: &sync.RWMutex{},

		workers: make(map[string]*worker),
		hr: make(chan *worker, 100),
	}

	// start up that hr team...
	go b.exitInterview()

	return b
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
func (b *Boss) HasWorker(name string) bool {
	b.RLock()
	defer b.RUnlock()

	if _, ok := b.workers[name]; ok {
		return true
	}

	return false
}

// NewWorker will attempt to create a new worker for a job. Returns error if you've already hired somebody.
func (b *Boss) NewWorker(name string, queueLength int) error {
	// see if we're already got somebody doin' it
	if b.HasWorker(name) {
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
	b.Lock()
	defer b.Unlock()

	// add worker
	b.workers[name] = newWorker(name, queueLength)

	// start up work routine
	go b.workers[name].doWork()

	// maybe say so?
	if debug {
		logger.Printf("Jobber: Go routine started for job \"%s\"\n", name)
	}

	return nil
}

// AddWork will push a new job to the end of a worker's queue
func (b *Boss) AddJob(workerName string, j Job) error {
	// see if we've already hired this worker
	if false == b.HasWorker(workerName) {
		if debug {
			logger.Printf("Jobber: No worker with \"%s\" found.\n", workerName)
		}
		return fmt.Errorf("No worker with name \"%s\" found", workerName)
	}

	// lock boss down
	b.RLock()
	defer b.RUnlock()

	// add to worker queue
	return b.workers[workerName].addJob(j)
}

// NewUnbufferedWorker will attempt to create a new worker with a queue length of 0
func (b *Boss) NewUnbufferedWorker(name string) error {
	return b.NewWorker(name, 0)
}

// StopWorker will tell a worker to finish up their queue then remove them
func (b *Boss) StopWorker(workerName string) error {
	if false == b.HasWorker(workerName) {
		if debug {
			logger.Printf("Jobber: No worker named \"%s\" found, cannot tell them to stop.", workerName)
		}
		return fmt.Errorf("No worker named \"%s\" found, cannot tell them to stop.", workerName)
	}

	b.RLock()
	defer b.RUnlock()

	err := b.workers[workerName].stop(b.hr)
	if nil != err {
		return err
	}

	return nil
}

// exitInterview processes workers coming in to hr
func (b *Boss) exitInterview() {
	for {
		select {
		case w := <-b.hr:
			logger.Printf("Jobber: Worker \"%s\" has completed all queued tasks.  They completed" +
				"\"%d\" jobs all told. Goodbye, \"%s\"...\n",
				w.name,
				w.completed,
				w.name)
			b.Lock()
			delete(b.workers, w.name)
			b.Unlock()
		}
	}
}