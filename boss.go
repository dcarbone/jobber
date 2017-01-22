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
	"fmt"
	"sync"
)

// Boss controls the life of the workers
type Boss struct {
	*sync.RWMutex

	workers map[string]*worker
	hr      chan *worker
}

// New creates a Boss
func NewBoss() *Boss {
	// initialize boss
	b := &Boss{
		RWMutex: &sync.RWMutex{},

		workers: make(map[string]*worker),
		hr:      make(chan *worker, 100),
	}

	// start up that hr team...
	go b.exitInterview()

	return b
}

// HasWorker lets you know if a worker already exists for a job
func (b *Boss) HasWorker(name string) bool {
	b.RLock()
	defer b.RUnlock()

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
			logger.Printf("Jobber: A worker for job \"%s\" already exists.\n", name)
		}
		return fmt.Errorf("A worker for job \"%s\" already exists.", name)
	}

	// 'lil input sanitizing
	if 0 > queueLength {
		// shout
		if debug {
			logger.Printf(
				"Jobber: Incoming new worker \"%s\" request specified invalid queue length of \"%d\","+
					" will set length to \"0\"\n",
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
			logger.Printf("Jobber: Worker \"%s\" has completed all queued tasks.  They completed"+
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
