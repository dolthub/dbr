package dbr

import (
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
)

type Job struct {
	event string
	kvs
	Exec func() error
}

func NewJob(event string, kvs kvs, exec func() error) *Job {
	return &Job{
		event: event,
		kvs:   kvs,
		Exec:  exec,
	}
}

type Queue struct {
	ctx    context.Context
	cancel context.CancelFunc
	eg     *errgroup.Group
	jobs   chan *Job
	log    EventReceiver
}

func (q *Queue) AddJob(job *Job) {
	q.jobs <- job
}

func (q *Queue) SetEventReciever(log EventReceiver) {
	q.log = log
}

func (q *Queue) Close() error {
	close(q.jobs)
	q.cancel()
	return q.eg.Wait()
}

func NewWorkingQueue(ctx context.Context, log EventReceiver) *Queue {
	ctx, cancel := context.WithCancel(ctx)
	eg, egCtx := errgroup.WithContext(ctx)
	q := &Queue{
		ctx:    egCtx,
		cancel: cancel,
		eg:     eg,
		jobs:   make(chan *Job),
		log:    log,
	}
	q.DoWork()
	return q
}

func (q *Queue) DoWork() {
	q.eg.Go(func() error {
		for {
			select {
			case <-q.ctx.Done():
				return q.ctx.Err()
			case j, ok := <-q.jobs:
				if !ok {
					return errors.New("failed to read job from queue")
				}
				err := j.Exec()
				if err != nil {
					// skip logging if theres no event name
					// let the job handle the loggin on its own
					if j.event != "" {
						if len(j.kvs) > 0 {
							q.log.EventErrKv(j.event, err, j.kvs)
						} else {
							q.log.EventErr(j.event, err)
						}
					}
				} else {
					// same here for success case
					if j.event != "" {
						if len(j.kvs) > 0 {
							q.log.EventKv(j.event, j.kvs)
						} else {
							q.log.Event(j.event)
						}
					}
				}

			}
		}
	})
}
