package main

import (
	"fmt"
	"github.com/dcarbone/jobber"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type importantJob struct {
	respondTo chan error
}

func main() {

	// Set up os signal channel
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	jobber.Debug()

	boss := jobber.NewBoss()

	boss.NewWorker("bob", 10)
	boss.NewUnbufferedWorker("jim")

	respChan := make(chan error, 10)

	go bob(boss, respChan)
	go jim(boss, respChan)

	for {
		select {
		case err := <-respChan:
			fmt.Printf("Received \"%v\" from response channel", err)
		case sig := <-sigChan:
			fmt.Printf("Received \"%s\", stopping...", sig)
			boss.StopWorker("bob")
			boss.StopWorker("jim")
			return
		}
	}
}

func (j *importantJob) Process() error {
	fmt.Println("I'm important...")
	return nil
}

func (j *importantJob) RespondTo() chan error {
	return j.respondTo
}

func bob(boss *jobber.Boss, respondTo chan error) {
	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-t.C:
			boss.AddJob("bob", &importantJob{respondTo: respondTo})
		}
	}
}

func jim(boss *jobber.Boss, respondTo chan error) {
	t := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-t.C:
			boss.AddJob("jim", &importantJob{respondTo: respondTo})
		}
	}
}
