package jobber

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Boss controls the life of the workers
type Boss struct {
	mu         *sync.Mutex
	shutdowned bool
	ctx        context.Context
	cancel     context.CancelFunc
	term       chan struct{}
	workers    map[string]*worker
	wg         *sync.WaitGroup
	hr         chan *worker
}

func NewBoss() *Boss {
	return newBoss(context.Background())
}

func NewBossWithContext(ctx context.Context) *Boss {
	return newBoss(ctx)
}

func newBoss(ctx context.Context) *Boss {
	b := &Boss{
		mu:      &sync.Mutex{},
		workers: make(map[string]*worker),
		hr:      make(chan *worker, 100),
		wg:      new(sync.WaitGroup),
	}
	b.ctx, b.cancel = context.WithCancel(ctx)

	go b.runner()

	return b
}

func (b *Boss) Terminate() {
	if b.shutdowned {
		return
	}
	close(b.term)
	b.wg.Wait()
}

func (b *Boss) Shutdown() {
	if b.shutdowned {
		return
	}
	b.cancel()
	b.wg.Wait()
}

func (b *Boss) Shutdowned() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.shutdowned
}

// HasWorker lets you know if a worker already exists for a job
func (b *Boss) HasWorker(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return false
	}
	_, ok := b.workers[name]
	return ok
}

// NewWorker will attempt to create a new worker for a job. Returns error if you've already hired somebody.
func (b *Boss) NewWorker(name string, queueLength int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[name]; ok {
		return fmt.Errorf("a worker for job \"%s\" already exists", name)
	}
	if 0 > queueLength {
		queueLength = 0
	}

	log.Printf("Creating new worker \"%s\" with queue length of \"%d\"", name, queueLength)

	b.wg.Add(1)
	b.workers[name] = newWorker(name, queueLength)
	go b.workers[name].doWork()

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
func (b *Boss) StopWorker(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[name]; ok {
		b.workers[name].stop(b.hr)
		return nil
	}
	return fmt.Errorf("worker named \"%s\" not found", name)
}

// AddWork will push a new job to the end of a worker's queue
func (b *Boss) AddJob(name string, j Job) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[name]; !ok {
		return fmt.Errorf("no worker named \"%s\" found", name)
	}
	return b.workers[name].addJob(j)
}

func (b *Boss) runner() {
	var term bool

Runner:
	for {
		select {
		case <-b.term:
			term = true
			break Runner
		case w := <-b.hr:
			b.mu.Lock()
			if debug {
				log.Printf("Worker \"%s\" has completed all queued tasks.  They completed \"%d\" jobs all told. Goodbye, \"%s\"...",
					w.name,
					w.completed,
					w.name)
			} else {
				log.Printf("Worker \"%s\" is closing", w.name)
			}
			delete(b.workers, w.name)
			b.mu.Unlock()
		case <-b.ctx.Done():
			log.Printf("Context done: %s", b.ctx.Err())
			break Runner
		}
	}

	b.mu.Lock()
	b.shutdowned = true
	b.mu.Unlock()

	if len(b.workers) > 0 {
		for _, w := range b.workers {
			if term {
				w.terminate(b.hr)
			} else {
				w.stop(b.hr)
			}
		}
	}

	for w := range b.hr {
		log.Printf("worker \"%s\" has stopped", w.name)
		b.wg.Done()
	}

	close(b.hr)
	b.workers = make(map[string]*worker)

	b.Shutdown()
}
