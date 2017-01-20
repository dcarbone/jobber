package dcarbone

type worker struct {
	name string

	jobs chan Job
}

func (jq *worker) doWork() {
	for {
		select {
		case j := <-jq.jobs:
			j.RespondTo() <- j.Process()
		}
	}
}

func (jq *worker) addWork(j Job) error {
	jq.jobs <- j
	return nil
}
