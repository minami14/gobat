package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/minami14/gobat"
)

func handler(ctx context.Context, retryCount int, prevError error) error {
	// The first time fails with a timeout and will be retried.
	timeout := time.Nanosecond
	if prevError != nil {
		// The second time it ends normally and the job ends.
		timeout = time.Second * 10
		log.Printf("retry: %v\n", retryCount)
		time.Sleep(time.Second * 10)
	}

	client := http.Client{
		Timeout: timeout,
	}

	resp, err := client.Get("https://example.com")
	if err != nil {
		log.Println(err)
		return err
	}
	defer resp.Body.Close()

	fmt.Println(resp.Status)
	return nil
}

func main() {
	runner := gobat.NewJobRunner(gobat.Config{})
	job := gobat.Job{
		Handler:       handler,
		MaxRetryCount: 2,
	}
	runner.Run(context.Background())
	runner.Submit(job)
	runner.GracefulShutdown()
	runner.Wait()
}
