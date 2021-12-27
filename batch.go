package gobat

import (
	"context"
	"sync"
	"time"
)

var (
	defaultConfig = Config{
		Parallels:             1,
		JobQueueSize:          1,
		DevideRetryQueue:      false,
		CheckShutdownInterval: time.Second,
	}
)

// JobHandler is a function executed by JobRunner.
type JobHandler func(ctx context.Context, retryCount int, prevError error) error

// Job is a job run by JobRunner
type Job struct {
	Handler       JobHandler
	MaxRetryCount int
	retryCount    int
	prevError     error
}

// Config for JobRunner.
type Config struct {
	Parallels             int
	JobQueueSize          int
	DevideRetryQueue      bool
	CheckShutdownInterval time.Duration
}

// JobRunner runs jobs.
type JobRunner struct {
	jobQueue   chan Job
	retryQueue chan Job
	conf       Config
	shutdown   bool
	wg         *sync.WaitGroup
}

// NewJobRunner creates new JobRunner.
func NewJobRunner(conf Config) *JobRunner {
	if conf.Parallels == 0 {
		conf.Parallels = defaultConfig.Parallels
	}
	if conf.JobQueueSize == 0 {
		conf.JobQueueSize = defaultConfig.JobQueueSize
	}
	if int64(conf.CheckShutdownInterval) == 0 {
		conf.CheckShutdownInterval = defaultConfig.CheckShutdownInterval
	}

	runner := &JobRunner{
		jobQueue: make(chan Job, conf.JobQueueSize),
		conf:     conf,
		wg:       new(sync.WaitGroup),
	}

	if conf.DevideRetryQueue {
		runner.retryQueue = make(chan Job, conf.JobQueueSize)
	}

	return runner
}

// Submit job to job queue.
func (j *JobRunner) Submit(job Job) {
	j.jobQueue <- job
}

func (j *JobRunner) submitRetry(job Job) {
	if j.conf.DevideRetryQueue {
		j.retryQueue <- job
	} else {
		j.jobQueue <- job
	}
}

func (j *JobRunner) runJob(ctx context.Context, job Job) {
	if err := job.Handler(ctx, job.retryCount, job.prevError); err != nil {
		job.prevError = err
		job.retryCount++
		if job.retryCount <= job.MaxRetryCount {
			j.submitRetry(job)
		}
	}
}

func (j *JobRunner) run(ctx context.Context, queue chan Job) {
	defer j.wg.Done()

	ticker := time.NewTicker(j.conf.CheckShutdownInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-queue:
			j.runJob(ctx, job)
		case <-ticker.C:
			if !j.shutdown || len(j.jobQueue) != 0 {
				continue
			}
			if !j.conf.DevideRetryQueue {
				return
			}
			if queue == j.retryQueue && len(j.retryQueue) == 0 {
				return
			}
			if queue == j.jobQueue && len(j.retryQueue) != 0 {
				queue = j.retryQueue
			}
		}
	}
}

// Run jobs.
func (j *JobRunner) Run(ctx context.Context) {
	if j.shutdown {
		j.shutdown = false
	}

	j.wg.Add(1)
	go j.run(ctx, j.jobQueue)

	if j.conf.DevideRetryQueue {
		j.wg.Add(1)
		go j.run(ctx, j.retryQueue)
	}
}

// RunParallel runs jobs in parralel.
func (j *JobRunner) RunParallel(ctx context.Context) {
	for i := 0; i < j.conf.Parallels; i++ {
		j.Run(ctx)
	}
}

// GracefulShutdown waits for all jobs to finish before existing JobRunner.
func (j *JobRunner) GracefulShutdown() {
	j.shutdown = true
}

// Wait for all jobs to finish.
func (j *JobRunner) Wait() {
	j.wg.Wait()
}
