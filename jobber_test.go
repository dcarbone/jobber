package jobber_test

import (
	"errors"
	"log"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dcarbone/jobber/v3"
)

const (
	defaultWorkerQueueLen     = 50
	defaultJobResponseChanLen = 5
)

var (
	jobnum        uint64
	expectedError = errors.New("i'm supposed to fail")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type (
	simpleJob struct {
		id          uint64
		resp        chan *jobber.JobResponse
		shouldErr   bool
		shouldPanic bool
	}
)

func newSimpleJob(shouldErr, shouldPanic bool, respChanLen int) *simpleJob {
	j := &simpleJob{resp: make(chan *jobber.JobResponse, respChanLen), shouldErr: shouldErr, shouldPanic: shouldPanic}
	j.id = atomic.AddUint64(&jobnum, 1)
	return j
}

func (j *simpleJob) Process() error {
	log.Printf("Job \"%d\" processing...", j.id)
	if j.shouldPanic {
		panic("all i did was try")
	}
	if j.shouldErr {
		return expectedError
	}
	time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
	return nil
}

func (j *simpleJob) RespondTo() chan<- *jobber.JobResponse {
	return j.resp
}

func newTestBoss(_ *testing.T) *jobber.Boss {
	return jobber.NewBoss(jobber.DefaultConfig())
}

func hireDan(t *testing.T, b *jobber.Boss, queueLen int) {
	if err := b.HireWorker("dan", queueLen); err != nil {
		t.Logf("Unable to hire worker: %s", err)
		t.FailNow()
	}
}

func TestBoss_HireWorker(t *testing.T) {
	b := newTestBoss(t)
	hireDan(t, b, defaultWorkerQueueLen)
}

func TestBoss_HasWorker(t *testing.T) {
	b := newTestBoss(t)
	hireDan(t, b, defaultWorkerQueueLen)
	hw := b.HasWorker("dan")
	if !hw {
		t.Log("Boss does not know about worker dan")
		t.FailNow()
	}
}

func TestBoss_AddJob(t *testing.T) {
	b := newTestBoss(t)
	hireDan(t, b, defaultWorkerQueueLen)
	j := newSimpleJob(false, false, defaultJobResponseChanLen)

	t.Run("CannotAddJobToUnknownWorker", func(t *testing.T) {
		err := b.AddJob("steve", j)
		if err == nil {
			t.Log("Expected error when attempting to add job to undefined worker")
			t.FailNow()
		}
	})

	t.Run("CanAddJobToKnownWorker", func(t *testing.T) {
		err := b.AddJob("dan", j)
		if err != nil {
			t.Logf("Unexpected error: %s", err)
			t.FailNow()
		}
	})
}

func TestWorker_PanicRecovery(t *testing.T) {
	b := newTestBoss(t)
	hireDan(t, b, defaultWorkerQueueLen)
	j := newSimpleJob(false, true, defaultJobResponseChanLen)

	t.Run("ShouldStayAlive", func(t *testing.T) {
		err := b.AddJob("dan", j)
		if err != nil {
			t.Logf("AddJob() error: %s", err)
			t.FailNow()
		}

		if !b.HasWorker("dan") {
			t.Log("dan should still be around")
			t.FailNow()
		}
	})

	t.Run("CanStillFire", func(t *testing.T) {
		b.ScaleDownAll()
		if b.HasWorker("dan") {
			t.Log("dan should not be around, all workers scaled down")
			t.FailNow()
		}
	})
}
