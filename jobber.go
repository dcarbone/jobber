package jobber

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

// Job represents any unit of work that you'd like to task a Worker with.  Any context handling should be done in your
// Process() implementation.
type Job interface {
	// Process must contain whatever logic is needed to perform the job, returning whatever error is generated while
	// processing (if any)
	Process() error
	// RespondTo must be passed whatever output came from Process()
	RespondTo() chan<- error
}

// HR is where workers are sent when they are done and should be removed from the Boss
type HR chan<- Worker

type Worker interface {
	// Name must return the name of worker.  This must be unique across all workers managed by the boss
	Name() string
	// Length must return the size of the current queue of work for this worker.
	Length() int
	// AddJob must attempt to add a new job to the worker's queue, failing if the worker has been told to stop
	AddJob(Job) error
	// ScaleDown must mark the worker as stopped, process any and all jobs remaining in it's queue, and then finally
	// send itself to HR
	ScaleDown()
	// Terminate must send an error message to all remaining jobs in this worker's queue, then send itself to HR.
	Terminate()
}

// PitDroids are simple workers that will do as instructed.
type PitDroid struct {
	mu         sync.RWMutex
	ctx        context.Context
	name       string
	jobs       chan Job
	terminated bool
	stopping   bool
	hr         HR
}

type HiringAgencyFunc func(ctx context.Context, name string, queueLength int, hr HR) Worker

// HiringAgency allows you to create your own worker hiring function in case you don't like PitDroids.
var HiringAgency HiringAgencyFunc = NewPitDroid

// NewPitDroid will return to you a new PitDroid, the default worker prototype for jobber
func NewPitDroid(ctx context.Context, name string, queueLength int, hr HR) Worker {
	w := PitDroid{
		name: name,
		ctx:  ctx,
		jobs: make(chan Job, queueLength),
		hr:   hr,
	}
	go w.work()
	return &w
}

// Name returns the name of this worker
func (w *PitDroid) Name() string {
	w.mu.RLock()
	n := w.name
	w.mu.RUnlock()
	return n
}

// Length returns the current number of items this worker has in its queue
func (w *PitDroid) Length() int {
	w.mu.RLock()
	l := len(w.jobs)
	w.mu.RUnlock()
	return l
}

// AddJob will append a job to this worker's queue
func (w *PitDroid) AddJob(j Job) (err error) {
	w.mu.RLock()
	if w.stopping {
		w.mu.RUnlock()
		err = fmt.Errorf("worker \"%s\" has been told to stop, cannot add new jobs", w.name)
	} else {
		w.mu.RUnlock()
		w.jobs <- j
	}
	return
}

// ScaleDown will tell this worker to stop accepting new jobs, complete all jobs left in its queue, then send itself to HR
func (w *PitDroid) ScaleDown() {
	w.mu.Lock()
	if !w.stopping {
		w.stopping = true
		close(w.jobs)
	}
	w.mu.Unlock()
}

// Terminate will tell this worker to stop accepting new jobs, flush all current jobs from its queue, then send itself to HR
func (w *PitDroid) Terminate() {
	w.mu.Lock()
	if !w.stopping {
		w.stopping = true
		w.terminated = true
		close(w.jobs)
	}
	w.mu.Unlock()
}

func (w *PitDroid) work() {
	var (
		terminated bool
		job        Job
		ok         bool
	)

	defer func(name string) {
		if r := recover(); r != nil {
			log.Printf("Worker %s had a job panic: %#v", name, r)
			log.Print("Trace:")
			log.Print(string(debug.Stack()))
			log.Printf("Sending %s back to work...", w.name)
			if job != nil {
				select {
				case job.RespondTo() <- fmt.Errorf("panic: %#v", r):
				default:
					log.Printf("!!! Unable to push to job %T response channel after panic!!!", job)
				}
			}
			go w.work() // only on panic recovery
		} else {
			w.hr <- w
		}
	}(w.name)

	for {
		select {
		case <-w.ctx.Done():
			err := w.ctx.Err()
			if errors.Is(err, context.Canceled) {
				log.Printf("! Worker %q: context has been cancelled, terminating...", w.name)
				w.Terminate()
			} else if errors.Is(err, context.DeadlineExceeded) {
				log.Printf("! Worker %q: context deadline exceeded, scaling down...", w.name)
				w.ScaleDown()
			} else {
				log.Printf("!! Worker %q: unknown context error seen, scaling down: %v", w.name, err)
				w.ScaleDown()
			}
		case job, ok = <-w.jobs:
			if !ok {
				log.Printf("! Worker %q: job queue has been closed, exiting run loop", w.name)
				return
			}
			w.mu.Lock()
			terminated = w.terminated
			w.mu.Unlock()
			// TODO: Don't like this, find better way
			if terminated {
				select {
				case job.RespondTo() <- fmt.Errorf("worker %q terminated", w.name): // try to respond, don't block whole worker if they aren't reading from queue or it's full.
				default: // fall on floor
					log.Printf("!!! Worker %q: Could not push to job %T response channel after worker termination!", w.name, job)
				}
			} else {
				res := job.Process()
				select {
				case job.RespondTo() <- res:
				default:
					log.Printf("!!! Worker %q: Could not push to job %T response channel!", w.name, job)
				}
			}
		}
	}
}

// Boss controls the life of the workers
type Boss struct {
	mu         sync.RWMutex
	terminated bool
	shutdowned bool
	ctx        context.Context
	cancel     context.CancelFunc
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
	ctx, cancel := context.WithCancel(ctx)
	b := Boss{
		workers: make(map[string]Worker),
		hr:      make(chan Worker, 100),
		wg:      new(sync.WaitGroup),
		ctx:     ctx,
		cancel:  cancel,
	}

	go b.runner()

	return &b
}

// Shutdown will attempt to gracefully shutdown, completing all currently queued jobs but no longer accepting new ones
func (b *Boss) Shutdown() {
	b.mu.Lock()
	if !b.shutdowned {
		b.shutdowned = true
		b.cancel()
	}
	b.mu.Unlock()
	b.wg.Wait()
}

// Terminate will immediately fire all workers and shut down the boss
func (b *Boss) Terminate() {
	b.mu.Lock()
	if !b.shutdowned {
		b.shutdowned = true
		b.terminated = true
		b.cancel()
	}
	b.mu.Unlock()
	b.wg.Wait()
}

// Shutdowned will return true if the boss has been told to shut down or terminate
func (b *Boss) Shutdowned() bool {
	b.mu.RLock()
	s := b.shutdowned
	b.mu.RUnlock()
	return s
}

func (b *Boss) HasWorker(name string) bool {
	b.mu.RLock()
	if b.shutdowned {
		b.mu.RUnlock()
		return false
	}
	_, ok := b.workers[name]
	b.mu.RUnlock()
	return ok
}

// Worker will attempt to return to you a worker by name
func (b *Boss) Worker(name string) (worker Worker) {
	b.mu.RLock()
	if !b.shutdowned {
		worker = b.workers[name]
	}
	b.mu.RUnlock()
	return
}

// HireWorker will attempt to hire a new worker using the specified HiringAgency and add them to the job pool.
func (b *Boss) HireWorker(ctx context.Context, name string, queueLength int) error {
	if 0 > queueLength {
		queueLength = 0
	}
	return b.PlaceWorker(HiringAgency(ctx, name, queueLength, b.hr))
}

// PlaceWorker will attempt to add a hired worker to the job pool, if one doesn't already exist with that name
func (b *Boss) PlaceWorker(worker Worker) (err error) {
	b.mu.Lock()
	if b.shutdowned {
		err = errors.New("boss is shutdowned")
	} else if _, ok := b.workers[worker.Name()]; ok {
		err = fmt.Errorf("worker %q already exists", worker.Name())
	} else {
		b.wg.Add(1)
		b.workers[worker.Name()] = worker
	}
	b.mu.Unlock()
	return
}

// AddWork will push a new job to a worker's queue
func (b *Boss) AddJob(workerName string, j Job) (err error) {
	b.mu.RLock()
	if b.shutdowned {
		err = errors.New("boss is shutdowned")
	} else if worker, ok := b.workers[workerName]; !ok {
		err = fmt.Errorf("worker %q found", workerName)
	} else {
		err = worker.AddJob(j)
	}
	b.mu.RUnlock()
	return
}

// ScaleDownWorker will tell a worker to finish up their queue then remove them
func (b *Boss) ScaleDownWorker(workerName string) (err error) {
	b.mu.RLock()
	if b.shutdowned {
		err = errors.New("boss is shutdowned")
	} else if worker, ok := b.workers[workerName]; ok {
		worker.ScaleDown()
	}
	b.mu.RUnlock()
	return
}

// TerminateWorker will remove the worker immediately, effectively cancelling all queued work.
func (b *Boss) TerminateWorker(workerName string) (err error) {
	b.mu.RLock()
	if b.shutdowned {
		err = errors.New("boss is shutdowned")
	} else if worker, ok := b.workers[workerName]; ok {
		worker.Terminate()
	}
	b.mu.RUnlock()
	return
}

func (b *Boss) runner() {
	var w Worker

runLoop:
	for {
		select {
		case w = <-b.hr:
			b.mu.Lock()
			delete(b.workers, w.Name())
			b.mu.Unlock()
		case <-b.ctx.Done():
			break runLoop
		}
	}

	// lock and mark as shutdowned
	b.mu.Lock()
	b.shutdowned = true
	terminated := b.terminated
	b.mu.Unlock()
	// range through all remaining workers and either terminate or scale down, depending...
	for _, w := range b.workers {
		if terminated {
			w.Terminate()
		} else {
			w.ScaleDown()
		}
	}
	// decrement wait group
	for range b.hr {
		b.wg.Done()
	}
	// close hr dept
	close(b.hr)
	// empty out map
	b.workers = nil

	// boss is now defunct.
}
