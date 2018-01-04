package jobber

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
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
		mu         sync.Mutex
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
	w.mu.Lock()
	n := w.name
	w.mu.Unlock()
	return n
}

func (w *PitDroid) Length() int {
	w.mu.Lock()
	l := len(w.jobs)
	w.mu.Unlock()
	return l
}

func (w *PitDroid) AddJob(j Job) error {
	w.mu.Lock()
	if w.stopping {
		w.mu.Unlock()
		return fmt.Errorf("worker \"%s\" has been told to stop, cannot add new jobs", w.name)
	}
	w.jobs <- j
	w.mu.Unlock()
	return nil
}

func (w *PitDroid) ScaleDown(hr HR) {
	w.mu.Lock()
	if w.stopping {
		w.mu.Unlock()
		return
	}
	w.stopping = true
	w.hr = hr
	close(w.jobs)
	w.mu.Unlock()
}

func (w *PitDroid) Terminate(hr HR) {
	w.mu.Lock()
	if w.stopping {
		w.mu.Unlock()
		return
	}
	w.terminated = true
	w.stopping = true
	w.hr = hr
	close(w.jobs)
	w.mu.Unlock()
}

func (w *PitDroid) work() {
	var terminated bool
	var job Job

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
				}
			}
			go w.work() // only on panic recovery
		} else {
			w.hr <- w
		}
	}(w.name)

	for job = range w.jobs {
		w.mu.Lock()
		terminated = w.terminated
		w.mu.Unlock()
		// TODO: Don't like this, find better way
		if terminated {
			select {
			case job.RespondTo() <- errors.New("worker terminated"): // try to respond, don't block whole worker if they aren't reading from queue or it's full.
			default: // fall on floor
			}
		} else {
			job.RespondTo() <- job.Process()
		}
	}
}

// Boss controls the life of the workers
type Boss struct {
	mu         sync.Mutex
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
	b := &Boss{
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
	b.mu.Lock()
	if b.shutdowned {
		b.mu.Unlock()
		return
	}
	b.shutdowned = true
	b.terminated = true
	b.cancel()
	b.mu.Unlock()
	b.wg.Wait()
}

// Shutdown will attempt to gracefully shutdown, completing all currently queued jobs but no longer accepting new ones
func (b *Boss) Shutdown() {
	b.mu.Lock()
	if b.shutdowned {
		b.mu.Unlock()
		return
	}
	b.shutdowned = true
	b.cancel()
	b.mu.Unlock()
	b.wg.Wait()
}

// Shutdowned will return true if the boss has been told to shut down or terminate
func (b *Boss) Shutdowned() bool {
	b.mu.Lock()
	s := b.shutdowned
	b.mu.Unlock()
	return s
}

func (b *Boss) HasWorker(name string) bool {
	b.mu.Lock()
	if b.shutdowned {
		b.mu.Unlock()
		return false
	}
	_, ok := b.workers[name]
	b.mu.Unlock()
	return ok
}

// Worker will attempt to return to you a worker by name
func (b *Boss) Worker(name string) (w Worker) {
	b.mu.Lock()
	if b.shutdowned {
		b.mu.Unlock()
		return nil
	}
	if w, ok := b.workers[name]; ok {
		b.mu.Unlock()
		return w
	}
	b.mu.Unlock()
	return nil
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
	if b.shutdowned {
		b.mu.Unlock()
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[w.Name()]; ok {
		b.mu.Unlock()
		return fmt.Errorf("a worker for job \"%s\" already exists", w.Name())
	}
	b.wg.Add(1)
	b.workers[w.Name()] = w
	b.mu.Unlock()
	return nil
}

// AddWork will push a new job to a worker's queue
func (b *Boss) AddJob(workerName string, j Job) error {
	b.mu.Lock()
	if b.shutdowned {
		b.mu.Unlock()
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[workerName]; !ok {
		b.mu.Unlock()
		return fmt.Errorf("no worker named \"%s\" found", workerName)
	}

	err := b.workers[workerName].AddJob(j)
	b.mu.Unlock()
	return err
}

// ScaleDownWorker will tell a worker to finish up their queue then remove them
func (b *Boss) ScaleDownWorker(name string) error {
	b.mu.Lock()
	if b.shutdowned {
		b.mu.Unlock()
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[name]; ok {
		b.workers[name].ScaleDown(b.hr)
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()
	return fmt.Errorf("worker named \"%s\" not found", name)
}

// TerminateWorker will remove the worker immediately, effectively cancelling all queued work.
func (b *Boss) TerminateWorker(name string) error {
	b.mu.Lock()
	if b.shutdowned {
		b.mu.Unlock()
		return errors.New("boss is shutdowned")
	}
	if _, ok := b.workers[name]; ok {
		b.workers[name].Terminate(b.hr)
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()
	return fmt.Errorf("worker named \"%s\" not found", name)
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
			w.Terminate(b.hr)
		} else {
			w.ScaleDown(b.hr)
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
