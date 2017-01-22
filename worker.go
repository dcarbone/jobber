package jobber

import "time"

type worker struct {
	name      string

	added uint64
	completed uint64

	jobs      chan Job
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
					"Jobber: \"%s\" job \"%d\" took \"%s\" nanoseconds to complete.\n",
					w.name,
					w.completed,
					time.Now().Sub(start).Nanoseconds())
			}
			// we've completed a job!
			w.completed++
		}
	}
}

// addJob appends this worker's queue with the incoming job
func (w *worker) addJob(j Job) error {
	if debug {
		logger.Printf("Jobber: Adding job \"%d\" to \"%s\" queue...", w.added, w.name)
	}
	w.jobs <- j
	w.added++
	return nil
}
