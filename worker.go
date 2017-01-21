package jobber

type worker struct {
	name string

	jobs chan Job
}

func (w *worker) doWork() {
	for {
		select {
		case j := <-w.jobs:
			j.RespondTo() <- j.Process()
		}
	}
}

func (w *worker) addWork(j Job) error {
	w.jobs <- j
	return nil
}
