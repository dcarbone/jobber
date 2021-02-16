package jobber

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var (
	jobID uint64

	WorkerStopping     = errors.New("worker is stopping")
	WorkerTerminated   = errors.New("worker terminated")
	WorkerJobQueueFull = errors.New("worker job queue full")
)

// JobResponse wraps the output of a job upon completion, whether it is successful or not.
type JobResponse struct {
	jobID   uint64
	JobType string
	Placed  time.Time
	Start   time.Time
	End     time.Time
	Worker  string
	Err     error
}

func handleResponse(log *log.Logger, workerName string, wj *workerJob, resp *JobResponse) {
	select {
	case wj.job.RespondTo() <- resp:
	case <-time.After(5 * time.Second):
		if log == nil {
			return
		}
		log.Printf("!!! Worker %q: unable to push to response chan for job of type %T", workerName, wj.job)
	}
}

// Job represents any unit of work that you'd like to task a Worker with.  Any context handling should be done in your
// Process() implementation.
type Job interface {
	// Process must contain whatever logic is needed to perform the job, returning whatever error is generated while
	// processing (if any)
	Process() error
	// RespondTo must be passed whatever output came from Process()
	RespondTo() chan<- *JobResponse
}

type workerJob struct {
	id     uint64
	placed time.Time
	ctx    context.Context
	job    Job
}

// HR is where workers are sent when they are done and should be removed from the Boss
type HR func(Worker)

type Worker interface {
	// Name must return the name of worker.  This must be unique across all workers managed by the boss
	Name() string
	// Length must return the size of the current queue of work for this worker.
	Length() int
	// AddJob must attempt to add a new job to the worker's queue, failing if the worker has been told to stop
	AddJob(context.Context, Job) error
	// ScaleDown must mark the worker as stopped, process any and all jobs remaining in it's queue
	ScaleDown()
	// Terminate must send an error message to all remaining jobs in this worker's queue
	Terminate()
}

type LoggingWorker interface {
	Worker
	SetLogger(*log.Logger)
}

type ManagedWorker interface {
	Worker
	SetHR(HR)
}

type WorkerStatistics struct {
	WorkerName            string    `json:"worker_name"`
	JobsCompleted         uint64    `json:"jobs_completed"`
	LastJobCompletionTime time.Time `json:"last_job_completion_time"`
}

type StatWorker interface {
	Worker
	Statistics() WorkerStatistics
}

// PitDroids are simple workers that will do as instructed.
type PitDroid struct {
	mu         sync.RWMutex
	log        *log.Logger
	name       string
	jobs       chan *workerJob
	terminated bool
	stopping   bool
	hr         HR

	completed uint64
	lastJob   time.Time
}

type HiringAgencyFunc func(name string, queueLength int) Worker

// HiringAgency allows you to create your own worker hiring function in case you don't like PitDroids.
var HiringAgency HiringAgencyFunc = NewPitDroid

// NewPitDroid will return to you a new PitDroid, the default worker prototype for jobber
func NewPitDroid(name string, queueLength int) Worker {
	w := PitDroid{
		name: name,
		jobs: make(chan *workerJob, queueLength),
	}
	go w.work()
	return &w
}

func (pd *PitDroid) logf(f string, v ...interface{}) {
	if pd.log == nil {
		return
	}
	pd.log.Printf(f, v...)
}

func (pd *PitDroid) SetLogger(log *log.Logger) {
	pd.log = log
}

func (pd *PitDroid) Statistics() WorkerStatistics {
	pd.mu.RLock()
	stats := WorkerStatistics{
		WorkerName:            pd.name,
		JobsCompleted:         pd.completed,
		LastJobCompletionTime: pd.lastJob,
	}
	pd.mu.RUnlock()
	return stats
}

// Name returns the name of this worker
func (pd *PitDroid) Name() string {
	n := pd.name
	return n
}

// Length returns the current number of items this worker has in its queue
func (pd *PitDroid) Length() int {
	pd.mu.RLock()
	l := len(pd.jobs)
	pd.mu.RUnlock()
	return l
}

// AddJob will append a job to this worker's queue.  The context is used to limit how long a worker can try to push
// a job onto its queue before failing.  It does not limit the job's execution time.
func (pd *PitDroid) AddJob(ctx context.Context, j Job) error {
	var err error

	pd.mu.RLock()
	if pd.stopping {
		pd.mu.RUnlock()
		err = WorkerStopping
	} else {
		pd.mu.RUnlock()
		select {
		case pd.jobs <- &workerJob{id: atomic.AddUint64(&jobID, 1), placed: time.Now(), ctx: ctx, job: j}:
		case <-ctx.Done():
			err = WorkerJobQueueFull
		}
	}

	return err
}

// ScaleDown will tell this worker to stop accepting new jobs, complete all jobs left in its queue, then send itself to HR
func (pd *PitDroid) ScaleDown() {
	pd.mu.Lock()
	if !pd.stopping {
		pd.stopping = true
		close(pd.jobs)
	}
	pd.mu.Unlock()
}

// Terminate will tell this worker to stop accepting new jobs, flush all current jobs from its queue, then send itself to HR
func (pd *PitDroid) Terminate() {
	pd.mu.Lock()
	if !pd.stopping {
		pd.stopping = true
		pd.terminated = true
		close(pd.jobs)
	}
	pd.mu.Unlock()
}

func (pd *PitDroid) work() {
	var (
		terminated bool
		resp       *JobResponse
		wj         *workerJob
	)

	defer func() {
		if r := recover(); r != nil {
			pd.logf("!!! Worker %q: panic during job execution: %#v", pd.name, r)
			pd.logf("!!! Worker %q: Trace:", pd.name)
			pd.logf(string(debug.Stack()))
			pd.logf("!!! Worker %q: restarting work after panic...", pd.name)
			if wj != nil {
				if resp == nil || wj.id != resp.jobID {
					resp = new(JobResponse)
				}
				resp.JobType = fmt.Sprintf("%T", wj.job)
				resp.Worker = pd.name
				resp.End = time.Now()
				resp.Err = fmt.Errorf("panic: %#v", r)
				select {
				case wj.job.RespondTo() <- resp:
				default:
					pd.logf("!!! Worker %q: unable to push to job %T response channel after panic!!!", pd.name, wj)
				}
			}
			resp = nil
			go pd.work() // only on panic recovery
		} else {
			if pd.hr != nil {
				pd.hr(pd)
				pd.logf("! Worker %q: pushed to HR", pd.name)
			}
		}
	}()

	for wj = range pd.jobs {
		pd.mu.Lock()
		terminated = pd.terminated
		pd.mu.Unlock()

		resp = new(JobResponse)
		resp.jobID = wj.id
		resp.JobType = fmt.Sprintf("%T", wj.job)
		resp.Worker = pd.name
		resp.Placed = wj.placed
		resp.Start = time.Now()

		// TODO: Don't like this, find better way
		if terminated {
			resp.End = time.Now()
			resp.Err = WorkerTerminated
		} else {
			res := wj.job.Process()
			resp.End = time.Now()
			resp.Err = res
		}

		go handleResponse(pd.log, pd.name, wj, resp)

		pd.mu.Lock()
		pd.completed++
		pd.lastJob = time.Now()
		pd.mu.Unlock()
	}
}

type BossConf struct {
	Log *log.Logger
}

func DefaultConfig() *BossConf {
	bc := new(BossConf)
	bc.Log = log.New(os.Stdout, "[jobber] ", log.LstdFlags)
	return bc
}

// Boss controls the life of the workers
type Boss struct {
	mu      sync.RWMutex
	log     *log.Logger
	workers map[string]Worker
}

// NewBoss will create a new Boss with a background context
func NewBoss(conf *BossConf) *Boss {
	b := Boss{
		workers: make(map[string]Worker),
	}

	if conf != nil && conf.Log != nil {
		b.log = conf.Log
		conf = nil
	}

	return &b
}

// ShutdownAll will attempt to gracefully shutdown, completing all currently queued jobs but no longer accepting new ones
func (b *Boss) ScaleDownAll() {
	b.mu.Lock()
	for name, w := range b.workers {
		delete(b.workers, name)
		w.ScaleDown()
	}
	b.workers = make(map[string]Worker)
	b.mu.Unlock()
}

// TerminateAll will immediately fire all current workers, dropping all currently queued jobs.
func (b *Boss) TerminateAll() {
	b.mu.Lock()
	for name, w := range b.workers {
		delete(b.workers, name)
		w.Terminate()
	}
	b.workers = make(map[string]Worker)
	b.mu.Unlock()
}

func (b *Boss) HasWorker(name string) bool {
	b.mu.RLock()
	_, ok := b.workers[name]
	b.mu.RUnlock()
	return ok
}

// Worker will attempt to return to you a worker by name
func (b *Boss) Worker(name string) (worker Worker) {
	b.mu.RLock()
	worker = b.workers[name]
	b.mu.RUnlock()
	return
}

// HireWorker will attempt to hire a new worker using the specified HiringAgency and add them to the job pool.
func (b *Boss) HireWorker(name string, queueLength int) error {
	if 0 > queueLength {
		queueLength = 0
	}
	return b.PlaceWorker(HiringAgency(name, queueLength))
}

// PlaceWorker will attempt to add a hired worker to the job pool, if one doesn't already exist with that name
func (b *Boss) PlaceWorker(worker Worker) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.workers[worker.Name()]; ok {
		return fmt.Errorf("worker %q already exists", worker.Name())
	}

	if lw, ok := worker.(LoggingWorker); ok {
		lw.SetLogger(b.log)
	}
	if mw, ok := worker.(ManagedWorker); ok {
		mw.SetHR(b.RemoveWorker)
	}

	b.workers[worker.Name()] = worker

	return nil
}

func (b *Boss) AddJobCtx(ctx context.Context, workerName string, j Job) error {
	var err error
	b.mu.RLock()
	if worker, ok := b.workers[workerName]; !ok {
		err = fmt.Errorf("worker not found: %q", workerName)
	} else {
		err = worker.AddJob(ctx, j)
	}
	b.mu.RUnlock()
	return err
}

// AddWork will push a new job to a worker's queue with no ttl
func (b *Boss) AddJob(workerName string, j Job) error {
	return b.AddJobCtx(context.Background(), workerName, j)
}

// ScaleDownWorker will tell a worker to finish up their queue then remove them
func (b *Boss) ScaleDownWorker(workerName string) error {
	var err error
	b.mu.Lock()
	if worker, ok := b.workers[workerName]; ok {
		worker.ScaleDown()
	}
	delete(b.workers, workerName)
	b.mu.Unlock()
	return err
}

// TerminateWorker will remove the worker immediately, effectively cancelling all queued work.
func (b *Boss) TerminateWorker(workerName string) (err error) {
	b.mu.Lock()
	if worker, ok := b.workers[workerName]; ok {
		worker.Terminate()
	}
	delete(b.workers, workerName)
	b.mu.Unlock()
	return
}

func (b *Boss) RemoveWorker(worker Worker) {
	b.mu.Lock()
	delete(b.workers, worker.Name())
	b.mu.Unlock()
}

// RemoveStaleWorkers terminate workers that have not processed a job after the provided deadline.  Note: this requires
// that workers implement the StatWorker interface.  Otherwise, this func does nothing but block for a short while.
func (b *Boss) RemoveStaleWorkers(deadline time.Time) int {
	b.mu.Lock()
	del := make([]string, 0)
	for name, w := range b.workers {
		if sw, ok := w.(StatWorker); ok {
			if sw.Statistics().LastJobCompletionTime.Before(deadline) {
				sw.Terminate()
				del = append(del, name)
			}
		}
	}
	for _, d := range del {
		delete(b.workers, d)
	}
	b.mu.Unlock()
	return len(del)
}
