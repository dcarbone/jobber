package jobber

import (
	"fmt"
)

type worker struct {
	name      string
	jobs      chan Job
	added     uint64
	completed uint64
	stopping  bool
	term      chan struct{}
	hr        chan<- *worker
}

func newWorker(name string, queueLength int) *worker {
	w := &worker{
		name: name,
		jobs: make(chan Job, queueLength),
		term: make(chan struct{}),
	}
	return w
}

func (w *worker) doWork() {
WorkLoop:
	for {
		select {
		case j, ok := <-w.jobs:
			if !ok {
				break WorkLoop
			}
			j.RespondTo() <- j.Process()
			w.completed++
		case <-w.term:
			break WorkLoop
		}
	}

	w.hr <- w
}

func (w *worker) addJob(j Job) error {
	if w.stopping {
		return fmt.Errorf("worker \"%s\" has been told to stop, cannot add new jobs", w.name)
	}
	if debug {
		log.Printf("Adding job \"%d\" to \"%s\" queue...", w.added, w.name)
	}
	w.jobs <- j
	w.added++
	return nil
}

func (w *worker) terminate(hr chan<- *worker) {
	if w.stopping {
		return
	}
	w.stopping = true
	w.hr = hr
	close(w.term)
}

func (w *worker) stop(hr chan<- *worker) {
	if w.stopping {
		return
	}
	w.stopping = true
	w.hr = hr
	close(w.jobs)
}
