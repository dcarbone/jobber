package main

import (
	"fmt"
	"github.com/dcarbone/jobber"
	"os"
	"os/signal"
	"syscall"
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
			fmt.Println(fmt.Sprintf("Received \"%v\" from response channel", err))
		case sig := <-sigChan:
			fmt.Println(fmt.Sprintf("Received \"%s\", stopping...", sig))
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

func bob(b *jobber.Boss, respondTo chan error) {
	for i := 0; i < 5; i++ {
		b.AddJob("bob", &importantJob{respondTo: respondTo})
	}
	b.StopWorker("bob")
}

func jim(b *jobber.Boss, respondTo chan error) {
	for i := 0; i < 5; i++ {
		b.AddJob("jim", &importantJob{respondTo: respondTo})
	}

	b.StopWorker("jim")
}
