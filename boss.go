package jobber

import (
	"fmt"
	"github.com/dcarbone/jobber/log"
	"sync"
)

// Boss controls the life of the workers
type Boss struct {
	m *sync.RWMutex

	workers map[string]*worker
	hr      chan *worker
}

// NewBoss creates a Boss
func NewBoss() *Boss {
	// initialize boss
	b := &Boss{
		m: &sync.RWMutex{},

		workers: make(map[string]*worker),
		hr:      make(chan *worker, 100),
	}

	// start up that hr team...
	go b.exitInterview()

	return b
}

// HasWorker lets you know if a worker already exists for a job
func (b *Boss) HasWorker(name string) bool {
	b.m.RLock()
	defer b.m.RUnlock()

	// do we have this person?
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
			log.Printf("A worker for job \"%s\" already exists.", name)
		}
		return fmt.Errorf("A worker for job \"%s\" already exists.", name)
	}

	// 'lil input sanitizing
	if 0 > queueLength {
		// shout
		if debug {
			log.Printf(
				"Incoming new worker \"%s\" request specified invalid queue length of \"%d\","+
					" will set length to \"0\"",
				name,
				queueLength)
		}
		// make unbuffered
		queueLength = 0
	}

	// tell the world
	log.Printf(
		"Creating new worker for job with name \"%s\" and queue length of \"%d\"",
		name,
		queueLength)

	// lock boss down
	b.m.Lock()
	defer b.m.Unlock()

	// add worker
	b.workers[name] = newWorker(name, queueLength)

	// start up work routine
	go b.workers[name].doWork()

	// maybe say so?
	if debug {
		log.Printf("Go routine started for job \"%s\"", name)
	}

	return nil
}

// NewUnbufferedWorker will attempt to create a new worker with a queue length of 0
func (b *Boss) NewUnbufferedWorker(name string) error {
	return b.NewWorker(name, 0)
}

// StopWorker will tell a worker to finish up their queue then remove them
func (b *Boss) StopWorker(workerName string) error {
	if false == b.HasWorker(workerName) {
		if debug {
			log.Printf("No worker named \"%s\" found, cannot tell them to stop.", workerName)
		}
		return fmt.Errorf("No worker named \"%s\" found, cannot tell them to stop.", workerName)
	}

	b.m.RLock()
	defer b.m.RUnlock()

	return b.workers[workerName].stop(b.hr)
}

// AddWork will push a new job to the end of a worker's queue
func (b *Boss) AddJob(workerName string, j Job) error {
	// see if we've already hired this worker
	if false == b.HasWorker(workerName) {
		if debug {
			log.Printf("No worker with \"%s\" found.", workerName)
		}
		return fmt.Errorf("No worker with name \"%s\" found", workerName)
	}

	// lock boss down
	b.m.RLock()
	defer b.m.RUnlock()

	// add to worker queue
	return b.workers[workerName].addJob(j)
}

// exitInterview processes workers coming in to hr
func (b *Boss) exitInterview() {
	for {
		w := <-b.hr
		log.Printf("Worker \"%s\" has completed all queued tasks.  They completed"+
			" \"%d\" jobs all told. Goodbye, \"%s\"...",
			w.name,
			w.completed,
			w.name)
		b.m.Lock()
		delete(b.workers, w.name)
		b.m.Unlock()
	}
}
