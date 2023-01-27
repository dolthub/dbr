package dbr

import (
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
	"sync"
)

type Job struct {
	wg    *sync.WaitGroup
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
	defer j.wg.Done()
	return j.exec()
}

type Queue struct {
	ctx      context.Context
	cancel   context.CancelFunc
	eg       *errgroup.Group
	wg       *sync.WaitGroup
	jobs     chan *Job
	stop     chan struct{}
	isClosed bool
	log      EventReceiver
}

func (q *Queue) AddJob(j *Job) error {
	if q.isClosed {
		return errors.New("failed to add job, secondary queue has been closed")
	}
	q.wg.Add(1)
	j.wg = q.wg
	q.jobs <- j
	return nil
}

func (q *Queue) SetEventReceiver(log EventReceiver) {
	q.log = log
}

func (q *Queue) Close() error {
	// stop allowing new jobs
	q.stop <- struct{}{}
	close(q.stop)

	// wait for outstanding jobs
	// to complete
	q.wg.Wait()

	// shut down job receiver
	q.cancel()

	close(q.jobs)
	return q.eg.Wait()
}

func NewWorkingQueue(ctx context.Context, buffer int, log EventReceiver) *Queue {
	ctx, cancel := context.WithCancel(ctx)
	eg, egCtx := errgroup.WithContext(ctx)
	q := &Queue{
		ctx:    egCtx,
		cancel: cancel,
		eg:     eg,
		wg:     &sync.WaitGroup{},
		jobs:   make(chan *Job, buffer),
		stop:   make(chan struct{}),
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
				return nil
			case <-q.stop:
				q.isClosed = true
				return nil
			default:
				continue
			}
		}
	})

	q.eg.Go(func() error {
		for {
			select {
			case <-q.ctx.Done():
				return nil
			case j, ok := <-q.jobs:
				if !ok {
					return errors.New("failed to read job from queue")
				}
				err := j.Run()
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
