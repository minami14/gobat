package main

import (
	"context"
	"fmt"
	"time"

	"github.com/minami14/gobat"
)

func handler(ctx context.Context, retryCount int, prevError error) error {
	// long io wait.
	fmt.Println("start job")
	time.Sleep(time.Second)
	fmt.Println("finish job")
	return nil
}

func main() {
	conf := gobat.Config{
		Parallels: 10,
	}
	runner := gobat.NewJobRunner(conf)
	runner.RunParallel(context.Background())

	for i := 0; i < 10; i++ {
		job := gobat.Job{
			Handler: handler,
		}
		runner.Submit(job)
	}

	runner.GracefulShutdown()
	runner.Wait()
}
