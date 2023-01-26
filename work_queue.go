package dbr

import (
	"context"
	"errors"
	"fmt"
)

type Job struct {
	Exec func() error
}

type Queue struct {
	ctx    context.Context
	cancel context.CancelFunc
	jobs   chan *Job
	errs   chan error
}

func (q *Queue) AddJob(job *Job) {
	fmt.Println("new job added to queue")
	q.jobs <- job
}

func (q *Queue) AddJobs(jobs []*Job) {
	for _, j := range jobs {
		q.AddJob(j)
	}
}

func (q *Queue) Close() {
	close(q.jobs)
	q.cancel()
}

func NewQueue() *Queue {
	ctx, cancel := context.WithCancel(context.Background())
	return &Queue{
		ctx:    ctx,
		cancel: cancel,
		jobs:   make(chan *Job),
		errs:   make(chan error),
	}
}

type Worker struct {
	q *Queue
}

func NewWorker(q *Queue) *Worker {
	return &Worker{q}
}

func (w *Worker) DoWork() error {
	defer close(w.q.errs)

	for {
		select {
		case <-w.q.ctx.Done():
			return w.q.ctx.Err()
		case j, ok := <-w.q.jobs:
			if !ok {
				return errors.New("failed to read job from queue")
			}
			w.q.errs <- j.Exec()
		}
	}
}
