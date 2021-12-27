package gobat_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/minami14/gobat"
)

type countMatcher struct {
	gomock.Matcher
	count int
}

func (m *countMatcher) Matches(v interface{}) bool {
	i, ok := v.(int)
	if !ok {
		return false
	}
	if i != m.count {
		return false
	}
	m.count++
	return true
}

func (m *countMatcher) String() string {
	return fmt.Sprintf("is equel to %v", m.count)
}

func TestJobRunner(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	retry := 5
	mockHandler := NewMockJobHandlerInterface(ctrl)
	mockHandler.EXPECT().Handler(gomock.Any(), &countMatcher{}, gomock.Any()).Return(errors.New("error")).Times(retry + 1)

	conf := gobat.Config{
		CheckShutdownInterval: time.Millisecond,
	}
	runner := gobat.NewJobRunner(conf)

	handler := func(ctx context.Context, retryCount int, prevError error) error {
		time.Sleep(time.Millisecond * 100)
		return mockHandler.Handler(ctx, retryCount, prevError)
	}
	job := gobat.Job{
		Handler:       handler,
		MaxRetryCount: retry,
	}

	runner.Submit(job)
	runner.Run(context.Background())
	runner.GracefulShutdown()
	runner.Wait()
}

func TestJobRunnerParallel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	retry := 5
	mockHandler := NewMockJobHandlerInterface(ctrl)
	mockHandler.EXPECT().Handler(gomock.Any(), &countMatcher{}, gomock.Any()).Return(errors.New("error")).Times(retry + 1)

	conf := gobat.Config{
		CheckShutdownInterval: time.Millisecond,
		Parallels:             3,
	}
	runner := gobat.NewJobRunner(conf)

	handler := func(ctx context.Context, retryCount int, prevError error) error {
		time.Sleep(time.Millisecond * 100)
		return mockHandler.Handler(ctx, retryCount, prevError)
	}
	job := gobat.Job{
		Handler:       handler,
		MaxRetryCount: retry,
	}

	runner.Submit(job)
	runner.RunParallel(context.Background())
	runner.GracefulShutdown()
	runner.Wait()
}

func TestJobRunnerDevideRetry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	retry := 5
	mockHandler := NewMockJobHandlerInterface(ctrl)
	mockHandler.EXPECT().Handler(gomock.Any(), &countMatcher{}, gomock.Any()).Return(errors.New("error")).Times(retry + 1)

	conf := gobat.Config{
		CheckShutdownInterval: time.Millisecond,
		DevideRetryQueue:      true,
	}
	runner := gobat.NewJobRunner(conf)

	handler := func(ctx context.Context, retryCount int, prevError error) error {
		t.Log(retryCount)
		time.Sleep(time.Millisecond * 100)
		return mockHandler.Handler(ctx, retryCount, prevError)
	}
	job := gobat.Job{
		Handler:       handler,
		MaxRetryCount: retry,
	}

	runner.Submit(job)
	runner.Run(context.Background())
	runner.GracefulShutdown()
	runner.Wait()
}

func TestJobRunnerDevideRetryParallel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	retry := 5
	mockHandler := NewMockJobHandlerInterface(ctrl)
	mockHandler.EXPECT().Handler(gomock.Any(), &countMatcher{}, gomock.Any()).Return(errors.New("error")).Times(retry + 1)

	conf := gobat.Config{
		CheckShutdownInterval: time.Millisecond,
		DevideRetryQueue:      true,
	}
	runner := gobat.NewJobRunner(conf)

	handler := func(ctx context.Context, retryCount int, prevError error) error {
		t.Log(retryCount)
		time.Sleep(time.Millisecond * 100)
		return mockHandler.Handler(ctx, retryCount, prevError)
	}
	job := gobat.Job{
		Handler:       handler,
		MaxRetryCount: retry,
	}

	runner.Submit(job)
	runner.RunParallel(context.Background())
	runner.GracefulShutdown()
	runner.Wait()
}

func TestJobRunnerNotError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	retry := 5
	mockHandler := NewMockJobHandlerInterface(ctrl)
	mockHandler.EXPECT().Handler(gomock.Any(), &countMatcher{}, gomock.Nil()).Return(nil).Times(1)

	conf := gobat.Config{
		CheckShutdownInterval: time.Millisecond,
	}
	runner := gobat.NewJobRunner(conf)

	handler := func(ctx context.Context, retryCount int, prevError error) error {
		t.Log(retryCount)
		time.Sleep(time.Millisecond * 100)
		return mockHandler.Handler(ctx, retryCount, prevError)
	}
	job := gobat.Job{
		Handler:       handler,
		MaxRetryCount: retry,
	}

	runner.Submit(job)
	runner.Run(context.Background())
	runner.GracefulShutdown()
	runner.Wait()
}
