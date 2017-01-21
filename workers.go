package jobber

import "sync"

var boss sync.Mutex

var workers map[string]*worker

func init() {
	boss = sync.RWMutex{}
	workers = make(map[string]*worker)
}

func Add(j Job) error {
	boss.Lock()
	defer boss.Unlock()

	if _, ok := workers[j.Name()]; !ok {
		workers[j.Name()] = hire(j.Name())
		go workers[j.Name()].doWork()
	}

	return workers[j.Name()].addWork(j)
}

func hire(name string) *worker {
	return &worker{
		name: name,
		jobs: make(chan Job),
	}
}
