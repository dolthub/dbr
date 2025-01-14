package dbr

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrSecondaryTxNotFound = errors.New("secondary tx not found")

// TxMpx is a multiplexed transaction created by SessionMpx.
type TxMpx struct {
	shouldSyncAtCommit bool
	enableDoubleReads  bool
	smpx               *SessionMpx
	PrimaryTx          *Tx
	SecondaryTx        *Tx
	SecondaryQ         *Queue
}

func (txMpx *TxMpx) AddJob(job *Job) error {
	return txMpx.SecondaryQ.AddJob(job)
}

func (txMpx *TxMpx) Wait() error {
	return txMpx.SecondaryQ.Wait()
}

func (txMpx *TxMpx) PrimaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return txMpx.PrimaryTx.ExecContext(ctx, query, args...)
}

func (txMpx *TxMpx) SecondaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if txMpx.SecondaryTx.Tx == nil {
		return nil, ErrSecondaryTxNotFound
	}
	return txMpx.SecondaryTx.ExecContext(ctx, query, args...)
}

func (txMpx *TxMpx) PrimaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return txMpx.PrimaryTx.QueryContext(ctx, query, args...)
}

func (txMpx *TxMpx) SecondaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if txMpx.SecondaryTx.Tx == nil {
		return nil, ErrSecondaryTxNotFound
	}
	return txMpx.SecondaryTx.QueryContext(ctx, query, args...)
}

// BeginTxs creates a transaction with TxOptions.
func (smpx *SessionMpx) BeginTxs(ctx context.Context, opts *sql.TxOptions) (*TxMpx, error) {
	to := smpx.GetTimeout()

	primaryTx, err := smpx.PrimaryConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, smpx.PrimaryConn.EventErr("dbr.primary.begin.error", err)
	}
	smpx.PrimaryConn.Event("dbr.primary.begin")

	secondaryCtx := NewContextWithMetricValues(ctx)

	q := NewWorkingQueue(secondaryCtx, 500, smpx.SecondaryEventReceiver)
	txmPx := &TxMpx{
		shouldSyncAtCommit: smpx.shouldSyncAtCommit,
		enableDoubleReads:  smpx.enableDoubleReads,
		PrimaryTx: &Tx{
			EventReceiver: smpx.PrimaryEventReceiver,
			Dialect:       smpx.PrimaryConn.Dialect,
			Tx:            primaryTx,
			Timeout:       to,
		},
		SecondaryTx: &Tx{
			EventReceiver: smpx.SecondaryEventReceiver,
			Dialect:       smpx.SecondaryConn.Dialect,
			Timeout:       to,
		},
		SecondaryQ: q,
		smpx:       smpx,
	}

	j := NewJob("dbr.secondary.begin", nil, func() error {
		secondaryTx, rerr := smpx.SecondaryConn.BeginTx(secondaryCtx, opts)
		if rerr != nil {
			return rerr
		}

		txmPx.SecondaryTx.Tx = secondaryTx
		return nil
	})

	// add job to the tx queue to ensure
	// all tx jobs have access to the sql.Tx
	err = q.AddJob(j)
	if err != nil {
		return nil, err
	}

	return txmPx, nil
}

// Begin creates a multiplexed transaction for the given session.
func (smpx *SessionMpx) Begin() (*TxMpx, error) {
	return smpx.BeginTxs(context.Background(), nil)
}

func (txMpx *TxMpx) GetTimeout() time.Duration {
	return txMpx.PrimaryTx.GetTimeout()
}

func (txMpx *TxMpx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return txMpx.ExecContext(context.Background(), query, args...)
}

func (txMpx *TxMpx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	primaryRes, err := txMpx.PrimaryTx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	j := NewJob("dbr.secondary.exec_context", map[string]string{"sql": query}, func() error {
		if txMpx.SecondaryTx.Tx == nil {
			return ErrSecondaryTxNotFound
		}

		_, err := txMpx.SecondaryTx.ExecContext(NewContextWithMetricValues(ctx), query, args...)
		return err
	})
	err = txMpx.AddJob(j)
	if err != nil {
		return nil, err
	}

	return primaryRes, nil
}

func (txMpx *TxMpx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	primaryRows, err := txMpx.PrimaryTx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	if txMpx.enableDoubleReads {
		j := NewJob("dbr.secondary.query_context", map[string]string{"sql": query}, func() (rerr error) {
			if txMpx.SecondaryTx.Tx == nil {
				rerr = ErrSecondaryTxNotFound
				return
			}

			var secondaryRows *sql.Rows
			secondaryRows, rerr = txMpx.SecondaryTx.QueryContext(NewContextWithMetricValues(ctx), query, args...)
			if rerr != nil {
				return
			}
			defer func() {
				cerr := secondaryRows.Close()
				if rerr == nil {
					rerr = cerr
				}
			}()

			// iterate secondary rows
			// which wont be returned
			for secondaryRows.Next() {
			}
			rerr = secondaryRows.Err()
			return
		})
		err = txMpx.AddJob(j)
		if err != nil {
			return nil, err
		}
	}

	return primaryRows, nil
}

// Commit finishes the transaction.
func (txMpx *TxMpx) Commit() error {
	// commit to the primary first
	// and base the commit to the secondary
	// on the success of the primary
	err := txMpx.PrimaryTx.Commit()

	// if the primary commit error is not nil
	// we need to rollback the secondary transaction.
	// we commit the secondary transaction only if the primary succeeded.
	j := NewJob("dbr.secondary.commit", nil, func() error {
		if txMpx.SecondaryTx.Tx == nil {
			return ErrSecondaryTxNotFound
		}
		if err == nil {
			return txMpx.SecondaryTx.Commit()
		}
		return txMpx.SecondaryTx.Rollback()
	})

	serr := txMpx.SecondaryQ.AddJobAndClose(j)
	if serr != nil {
		return serr
	}

	// if true, block on secondary commit
	if txMpx.shouldSyncAtCommit {
		werr := txMpx.SecondaryQ.Wait()
		if werr != nil {
			txMpx.SecondaryTx.EventErr("dbr.secondary.queue.wait", werr)
			return werr
		}
	} else {
		go func() {
			werr := txMpx.SecondaryQ.Wait()
			if werr != nil {
				txMpx.SecondaryTx.EventErr("dbr.secondary.queue.wait", werr)
			}
		}()
	}

	if err != nil {
		return txMpx.PrimaryTx.EventErr("dbr.primary.commit.error", err)
	}
	txMpx.PrimaryTx.Event("dbr.primary.commit")
	return nil
}

// Rollback cancels the transaction.
func (txMpx *TxMpx) Rollback() error {
	j := NewJob("dbr.secondary.rollback", nil, func() error {
		if txMpx.SecondaryTx.Tx == nil {
			return ErrSecondaryTxNotFound
		}
		return txMpx.SecondaryTx.Rollback()
	})

	err := txMpx.SecondaryQ.AddJobAndClose(j)
	if err != nil {
		return err
	}

	go func() {
		werr := txMpx.SecondaryQ.Wait()
		if werr != nil {
			txMpx.SecondaryTx.EventErr("dbr.secondary.queue.wait", werr)
		}
	}()

	err = txMpx.PrimaryTx.Rollback()
	if err != nil {
		return txMpx.PrimaryTx.EventErr("dbr.primary.rollback", err)
	}
	txMpx.PrimaryTx.Event("dbr.primary.rollback")

	return nil
}

// RollbackUnlessCommitted rollsback the multiplexed transaction unless
// it has already been committed or rolled back.
//
// Useful to defer tx.RollbackUnlessCommitted(), so you don't
// have to handle N failure cases.
// Keep in mind the only way to detect an error on the rollback
// is via the event log.
func (txMpx *TxMpx) RollbackUnlessCommitted() {
	j := &Job{exec: func() error {
		if txMpx.SecondaryTx.Tx == nil {
			return ErrSecondaryTxNotFound
		}

		err := txMpx.SecondaryTx.Rollback()
		if err == sql.ErrTxDone || err == errFailedToAddJob {
			// ok
		} else if err != nil {
			txMpx.SecondaryTx.EventErr("dbr.secondary.rollback.rollback_unless_committed", err)
		} else {
			txMpx.SecondaryTx.Event("dbr.secondary.rollback")
		}
		return nil
	}}

	err := txMpx.SecondaryQ.AddJobAndClose(j)
	if err != nil && err != errFailedToAddJob {
		txMpx.SecondaryTx.EventErr("dbr.secondary.add_and_close.rollback_unless_committed", err)
	}

	go func() {
		werr := txMpx.SecondaryQ.Wait()
		if werr != nil {
			txMpx.SecondaryTx.EventErr("dbr.secondary.queue.wait", werr)
		}
	}()

	err = txMpx.PrimaryTx.Rollback()
	if err == sql.ErrTxDone {
		// ok
	} else if err != nil {
		txMpx.PrimaryTx.EventErr("dbr.primary.rollback_unless_committed", err)
		return
	} else {
		txMpx.PrimaryTx.Event("dbr.primary.rollback")
	}
}
