package dbr

import (
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
	"sync"
	"sync/atomic"
)

const (
	firstOccurrence = "__first_occurrence__"
)

type Job struct {
	event string
	kvs
	exec func() error
}

func NewJob(event string, kvs kvs, exec func() error) *Job {
	return &Job{
		event: event,
		kvs:   kvs,
		exec:  exec,
	}
}

func (j *Job) Run() error {
	return j.exec()
}

type Queue struct {
	ctx      context.Context
	eg       *errgroup.Group
	m        *sync.Mutex
	jobs     chan *Job
	isClosed *atomic.Bool
	log      EventReceiver
}

func (q *Queue) AddJob(j *Job) error {
	if q.isClosed.Load() {
		return errors.New("failed to add job, secondary queue has been closed")
	}
	q.m.Lock()
	defer q.m.Unlock()
	q.jobs <- j
	return nil
}

func (q *Queue) SetEventReceiver(log EventReceiver) {
	q.m.Lock()
	defer q.m.Unlock()
	q.log = log
}

func (q *Queue) Close() error {
	q.m.Lock()
	defer q.m.Unlock()

	q.isClosed.Swap(true)

	close(q.jobs)
	return q.eg.Wait()
}

func NewWorkingQueue(ctx context.Context, buffer int, log EventReceiver) *Queue {
	eg, egCtx := errgroup.WithContext(ctx)
	b := &atomic.Bool{}
	b.Store(false)
	q := &Queue{
		ctx:      egCtx,
		eg:       eg,
		m:        &sync.Mutex{},
		jobs:     make(chan *Job, buffer),
		log:      log,
		isClosed: b,
	}
	q.DoWork()
	return q
}

func (q *Queue) DoWork() {
	q.eg.Go(func() error {
		count := 0
		for {
			select {
			case <-q.ctx.Done():
				return nil
			case j, ok := <-q.jobs:
				if !ok {
					return nil
				}

				err := j.Run()
				if err != nil {
					var m kvs

					if len(j.kvs) > 0 {
						m = j.kvs
					} else {
						m = make(kvs)
					}

					// if this is the first error
					// mark it as such for auditing
					if count == 1 {
						m[firstOccurrence] = "true"
					}

					eventName := j.event
					if eventName == "" {
						eventName = "dbr.secondary.job.failure"
					}

					if len(m) > 0 {
						q.log.EventErrKv(eventName, err, m)
					} else {
						q.log.EventErr(eventName, err)
					}
				} else {
					if j.event != "" {
						if len(j.kvs) > 0 {
							q.log.EventKv(j.event, j.kvs)
						} else {
							q.log.Event(j.event)
						}
					}
				}

				count++
			}
		}
	})
}
