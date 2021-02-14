package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dcarbone/jobber/v2"
)

type importantJob struct {
	id        int
	respondTo chan error
}

func (j *importantJob) Process() error {
	log.Printf("Important job %d processing here!", j.id)
	return nil
}

func (j *importantJob) RespondTo() chan<- error {
	return j.respondTo
}

type lessImportantJob struct {
	id        int
	respondTo chan error
}

func (j *lessImportantJob) Process() error {
	log.Printf("Less Important job %d processing here!", j.id)
	return nil
}

func (j *lessImportantJob) RespondTo() chan<- error {
	return j.respondTo
}

func main() {

	// Set up os signal channel
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	boss := jobber.NewBoss()

	_ = boss.HireWorker(context.Background(), "bob", 10)
	_ = boss.HireWorker(context.Background(), "jim", 0)

	respChan := make(chan error, 20)

	go bob(boss, respChan)
	go jim(boss, respChan)

	i := 0

	for {
		select {
		case err := <-respChan:
			fmt.Println(fmt.Sprintf("Received \"%v\" from response channel", err))
			i++
			if i >= 20 {
				boss.Shutdown()
				return
			}
		case sig := <-sigChan:
			fmt.Println(fmt.Sprintf("Received \"%s\", stopping...", sig))
			return
		}
	}
}

func bob(b *jobber.Boss, respondTo chan error) {
	for i := 0; i < 10; i++ {
		_ = b.AddJob("bob", &lessImportantJob{id: i, respondTo: respondTo})
	}
}

func jim(b *jobber.Boss, respondTo chan error) {
	for i := 0; i < 10; i++ {
		_ = b.AddJob("jim", &importantJob{id: i, respondTo: respondTo})
	}
}
