package jobber

import (
	"sync"
	"fmt"
)

var boss sync.RWMutex

var workers map[string]*worker

func init() {
	boss = sync.RWMutex{}
	workers = make(map[string]*worker)
}

func NewWorker(jobName string, queueLength int) error {
	boss.Lock()
	defer boss.Unlock()

	if _, ok := workers[jobName]; ok {
		return fmt.Errorf("Worker with name \"%s\" already exists.", jobName)
	}

	if 0 > queueLength {
		queueLength = 0
	}

	workers[jobName] = &worker{
		name: jobName,
		jobs: make(chan Job, queueLength),
	}

	go workers[jobName].doWork()

	return nil
}

func AddJob(j Job) error {
	boss.RLock()
	defer boss.RUnlock()

	worker, ok := workers[j.Name()]
	if !ok {
		return fmt.Errorf("No worker assigned to \"%s\"", j.Name())
	}

	return worker.addWork(j)
}

