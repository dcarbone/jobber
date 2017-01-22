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
	"time"
)

type worker struct {
	name string
	jobs chan Job

	added     uint64
	completed uint64

	stopLock sync.Mutex
	stopping bool
	hr       chan *worker
}

func newWorker(name string, queueLength int) *worker {
	return &worker{
		name: name,
		jobs: make(chan Job, queueLength),

		stopLock: sync.Mutex{},
	}
}

// doWork does just that.
func (w *worker) doWork() {
	var start time.Time
	for {
		select {
		case j := <-w.jobs:
			// Only track time to completion if they've enabled debug mode.
			if debug {
				start = time.Now()
				logger.Printf("Jobber: Processing \"%s\" job \"%d\"...\n", w.name, w.completed)
			}
			// process job from queue
			j.RespondTo() <- j.Process()
			// if debugging, print duration stats
			if debug {
				logger.Printf(
					"Jobber: \"%s\" job \"%d\" took \"%d\" nanoseconds to complete.\n",
					w.name,
					w.completed,
					time.Now().Sub(start).Nanoseconds())
			}
			// we've completed a job!
			w.completed++

			// check if we've been told to stop...
			if w.stopping && 0 == len(w.jobs) {
				// walk ourselves to hr...
				w.hr <- w
				return
			}
		}
	}
}

// addJob appends this worker's queue with the incoming job
func (w *worker) addJob(j Job) error {
	if w.stopping {
		logger.Printf("Jobber: Worker \"%s\" has been told to stop, cannot add new jobs.\n", w.name)
		return fmt.Errorf("Worker \"%s\" has been told to stop, cannot add new jobs.", w.name)
	}
	if debug {
		logger.Printf("Jobber: Adding job \"%d\" to \"%s\" queue...", w.added, w.name)
	}
	w.jobs <- j
	w.added++
	return nil
}

// stop will tell the worker to complete it's current task list then shut down...
func (w *worker) stop(hr chan *worker) error {
	// if i've already been told to stop..
	if w.stopping {
		logger.Printf("Jobber: Worker \"%s\" has already been told to stop.", w.name)
		return fmt.Errorf("Worker \"%s\" has already been told to stop.", w.name)
	}

	// don't tell me more than once!
	w.stopLock.Lock()
	defer w.stopLock.Unlock()

	// i'm stoppin...
	w.stopping = true
	w.hr = hr

	// tell the world
	logger.Printf("Jobber: Stopping worker \"%s\"...\n", w.name)

	return nil
}
