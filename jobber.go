package jobber

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type (
	// Job represents any unit of work that you'd like to task a DefaultWorker with.  Any context handling should be done in your
	// Process() implementation.
	Job interface {
		// Process must contain whatever logic is needed to perform the job, returning whatever error is generated while
		// processing (if any)
		Process() error
		// RespondTo must be passed whatever output came from Process()
		RespondTo() chan<- error
	}

	// HR is where workers are sent when they are done and should be removed from the Boss
	HR chan<- Worker

	Worker interface {
		// Name must return the name of worker.  This must be unique across all workers managed by the boss
		Name() string
		// Length must return the size of the current queue of work for this worker.
		Length() int
		// AddJob must attempt to add a new job to the worker's queue, failing if the worker has been told to stop
		AddJob(Job) error
		// ScaleDown must mark the worker as stopped, process any and all jobs remaining in it's queue, and then finally
		// send itself to HR
		ScaleDown(HR)
		// Terminate must send an error message to all remaining jobs in this worker's queue, then send itself to HR.
		Terminate(HR)
	}

	// PitDroids are simple workers that will do as instructed.
	PitDroid struct {
		mu         *sync.Mutex
		name       string
		jobs       chan Job
		terminated bool
		stopping   bool
		hr         HR
	}
)

// NewPitDroid will return to you a new PitDroid, the default worker prototype for jobber
func NewPitDroid(name string, queueLength int) Worker {
	w := &PitDroid{
		mu:   new(sync.Mutex),
		name: name,
		jobs: make(chan Job, queueLength),
	}
	go w.work()
	return w
}

type HiringAgencyFunc func(name string, queueLength int) Worker

// HiringAgency allows you to create your own worker hiring function in case you don't like PitDroids.
var HiringAgency HiringAgencyFunc = NewPitDroid

func (w *PitDroid) Name() string {
	return w.name
}

func (w *PitDroid) AddJob(j Job) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopping {
		return fmt.Errorf("worker \"%s\" has been told to stop, cannot add new jobs", w.name)
	}
	w.jobs <- j
	return nil
}

func (w *PitDroid) Length() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.jobs)
}

func (w *PitDroid) ScaleDown(hr HR) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopping {
		return
	}
	w.stopping = true
	w.hr = hr
	close(w.jobs)
}

func (w *PitDroid) Terminate(hr HR) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopping {
		return
	}
	w.terminated = true
	w.stopping = true
	w.hr = hr
	close(w.jobs)
}

func (w *PitDroid) work() {
	for j := range w.jobs {
		// TODO: Don't like this, find better way
		if w.terminated {
			j.RespondTo() <- errors.New("worker terminated")
		} else {
			j.RespondTo() <- j.Process()
		}
	}
	w.hr <- w
}

// Boss controls the life of the workers
type Boss struct {
	mu         *sync.Mutex
	shutdowned bool
	ctx        context.Context
	cancel     context.CancelFunc
	term       chan struct{}
	workers    map[string]Worker
	wg         *sync.WaitGroup
	hr         chan Worker
}

// NewBoss will create a new Boss with a background context
func NewBoss() *Boss {
	return newBoss(context.Background())
}

// NewBossWithContext will create a new Boss with a context of your creation
func NewBossWithContext(ctx context.Context) *Boss {
	return newBoss(ctx)
}

func newBoss(ctx context.Context) *Boss {
	b := &Boss{
		mu:      new(sync.Mutex),
		workers: make(map[string]Worker),
		hr:      make(chan Worker, 100),
		wg:      new(sync.WaitGroup),
	}
	b.ctx, b.cancel = context.WithCancel(ctx)

	go b.runner()

	return b
}

// Terminate will immediately fire all workers and shut down the boss
func (b *Boss) Terminate() {
	if b.shutdowned {
		return
	}
	close(b.term)
	b.wg.Wait()
}

// Shutdown will attempt to gracefully shutdown, completing all currently queued jobs but no longer accepting new ones
func (b *Boss) Shutdown() {
	if b.shutdowned {
		return
	}
	b.cancel()
	b.wg.Wait()
}

// Shutdowned will return true if the boss has been told to shut down or terminate
func (b *Boss) Shutdowned() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.shutdowned
}

func (b *Boss) HasWorker(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return false
	}
	_, ok := b.workers[name]
	return ok
}

// Worker will attempt to return to you a worker by name
func (b *Boss) Worker(name string) Worker {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return nil
	}
	if w, ok := b.workers[name]; ok {
		return w
	} else {
		return nil
	}
}

// HireWorker will attempt to hire a new worker using the specified HiringAgency and add them to the job pool.
func (b *Boss) HireWorker(name string, queueLength int) error {
	if 0 > queueLength {
		queueLength = 0
	}
	return b.PlaceWorker(HiringAgency(name, queueLength))
}

// PlaceWorker will attempt to add a hired worker to the job pool, if one doesn't already exist with that name
func (b *Boss) PlaceWorker(w Worker) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[w.Name()]; ok {
		return fmt.Errorf("a worker for job \"%s\" already exists", w.Name())
	}
	b.wg.Add(1)
	b.workers[w.Name()] = w
	return nil
}

// AddWork will push a new job to a worker's queue
func (b *Boss) AddJob(workerName string, j Job) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[workerName]; !ok {
		return fmt.Errorf("no worker named \"%s\" found", workerName)
	}
	return b.workers[workerName].AddJob(j)
}

// ScaleDownWorker will tell a worker to finish up their queue then remove them
func (b *Boss) ScaleDownWorker(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[name]; ok {
		b.workers[name].ScaleDown(b.hr)
		return nil
	}
	return fmt.Errorf("worker named \"%s\" not found", name)
}

// TerminateWorker will remove the worker immediately, effectively cancelling all queued work.
func (b *Boss) TerminateWorker(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdowned {
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[name]; ok {
		b.workers[name].Terminate(b.hr)
		return nil
	}
	return fmt.Errorf("worker named \"%s\" not found", name)
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
			delete(b.workers, w.Name())
			b.mu.Unlock()
		case <-b.ctx.Done():
			break Runner
		}
	}

	b.mu.Lock()
	b.shutdowned = true
	b.mu.Unlock()

	if len(b.workers) > 0 {
		for _, w := range b.workers {
			if term {
				w.Terminate(b.hr)
			} else {
				w.ScaleDown(b.hr)
			}
		}
	}

	for range b.hr {
		b.wg.Done()
	}

	close(b.hr)
	b.workers = make(map[string]Worker)

	b.Shutdown()
}
