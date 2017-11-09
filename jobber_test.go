package jobber_test

import (
	"errors"
	"github.com/dcarbone/jobber"
	"log"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
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
		id        uint64
		resp      chan error
		shouldErr bool
	}
)

func newSimpleJob(shouldErr bool) *simpleJob {
	j := &simpleJob{resp: make(chan error), shouldErr: shouldErr}
	j.id = atomic.AddUint64(&jobnum, 1)
	return j
}

func (j *simpleJob) Process() error {
	log.Printf("Job \"%d\" processing...", j.id)
	if j.shouldErr {
		return expectedError
	} else {
		time.Sleep(time.Duration(rand.Intn(3)) * time.Second)
		return nil
	}
}

func (j *simpleJob) RespondTo() chan<- error {
	return j.resp
}

func newTestBoss(_ *testing.T) *jobber.Boss {
	return jobber.NewBoss()
}

func hireDan(t *testing.T, b *jobber.Boss) {
	err := b.HireWorker("dan", 50)
	if err != nil {
		t.Logf("Unable to hire worker: %s", err)
		t.FailNow()
	}
}

func TestBoss_HireWorker(t *testing.T) {
	b := newTestBoss(t)
	hireDan(t, b)
}

func TestBoss_HasWorker(t *testing.T) {
	b := newTestBoss(t)
	hireDan(t, b)
	hw := b.HasWorker("dan")
	if !hw {
		t.Log("Boss does not know about worker dan")
		t.FailNow()
	}
}

func TestBoss_AddJob(t *testing.T) {
	b := newTestBoss(t)
	hireDan(t, b)
	j := newSimpleJob(false)

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
