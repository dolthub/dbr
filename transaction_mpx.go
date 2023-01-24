package dbr

import (
	"context"
	"database/sql"
	"golang.org/x/sync/errgroup"
	"time"
)

// TxMpx is a multiplexed transaction created by SessionMpx.
type TxMpx struct {
	PrimaryTx   *Tx
	SecondaryTx *Tx
}

func (txMpx *TxMpx) GetTimeout() time.Duration {
	return txMpx.PrimaryTx.GetTimeout()
}

func (txMpx *TxMpx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return txMpx.PrimaryTx.ExecContext(ctx, query, args...)
}

func (txMpx *TxMpx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return txMpx.PrimaryTx.QueryContext(ctx, query, args...)
}

func (txMpx *TxMpx) GetPrimaryTimeout() time.Duration {
	return txMpx.PrimaryTx.Timeout
}

func (txMpx *TxMpx) GetSecondaryTimeout() time.Duration {
	return txMpx.SecondaryTx.Timeout
}

func (txMpx *TxMpx) PrimaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return txMpx.PrimaryTx.ExecContext(ctx, query, args...)
}

func (txMpx *TxMpx) SecondaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return txMpx.SecondaryTx.ExecContext(ctx, query, args...)
}

func (txMpx *TxMpx) PrimaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return txMpx.PrimaryTx.QueryContext(ctx, query, args...)
}

func (txMpx *TxMpx) SecondaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return txMpx.SecondaryTx.QueryContext(ctx, query, args...)
}

type AsyncTx struct {
	Tx      *Tx
	ErrChan chan error
}

type AsyncTxChan chan *AsyncTx

// BeginTxs creates a transaction with TxOptions.
func (smpx *SessionMpx) BeginTxs(ctx context.Context, opts *sql.TxOptions) (*Tx, AsyncTxChan, error) {
	primaryTx, err := smpx.PrimaryConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, nil, smpx.PrimaryConn.EventErr("dbr.primary.begin.error", err)
	}
	smpx.PrimaryConn.Event("dbr.primary.begin")

	secondaryErrChan := make(chan error)

	secondaryAsyncTxChan := make(AsyncTxChan)
	eg, egCtx := errgroup.WithContext(ctx)

	go func() {
		defer close(secondaryErrChan)
		secondaryErrChan <- eg.Wait()
	}()

	eg.Go(func() error {
		defer close(secondaryAsyncTxChan)

		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			default:
				secondaryTx, rerr := smpx.SecondaryConn.BeginTx(ctx, opts)
				if rerr != nil {
					return smpx.SecondaryConn.EventErr("dbr.secondary.begin.error", rerr)
				}

				smpx.SecondaryConn.Event("dbr.secondary.begin")

				s := &Tx{
					EventReceiver: smpx.SecondaryEventReceiver,
					Dialect:       smpx.SecondaryConn.Dialect,
					Tx:            secondaryTx,
					Timeout:       smpx.GetSecondaryTimeout(),
				}

				secondaryAsyncTxChan <- &AsyncTx{
					Tx:      s,
					ErrChan: secondaryErrChan,
				}

				return nil
			}
		}
	})

	p := &Tx{
		EventReceiver: smpx.PrimaryEventReceiver,
		Dialect:       smpx.PrimaryConn.Dialect,
		Tx:            primaryTx,
		Timeout:       smpx.GetPrimaryTimeout(),
	}

	return p, secondaryAsyncTxChan, nil
}

// Begin creates a multiplexed transaction for the given session.
func (smpx *SessionMpx) Begin() (*Tx, AsyncTxChan, error) {
	return smpx.BeginTxs(context.Background(), nil)
}

// ExecContextMpx executes queries against the multiplexed transaction
func (txMpx *TxMpx) ExecContextMpx(ctx context.Context, query string, args ...interface{}) (sql.Result, AsyncResultChan, error) {
	primaryResults, err := txMpx.PrimaryTx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}

	secondaryErrChan := make(chan error)

	secondaryResultChan := make(AsyncResultChan)
	eg, egCtx := errgroup.WithContext(ctx)

	go func() {
		defer close(secondaryErrChan)
		secondaryErrChan <- eg.Wait()
	}()

	eg.Go(func() error {
		defer close(secondaryResultChan)

		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			default:
				secondaryResults, rerr := txMpx.SecondaryTx.ExecContext(ctx, query, args...)
				if rerr != nil {
					return rerr
				}
				ar := &AsyncResult{
					Query:   query,
					ErrChan: secondaryErrChan,
					Result:  secondaryResults,
				}
				secondaryResultChan <- ar
				return nil
			}
		}
	})

	return primaryResults, secondaryResultChan, nil
}

// Commit finishes the primary transaction and returns an error channel for the secondary transaction committed asynchronously.
func (txMpx *TxMpx) Commit() (error, chan error) {
	err := txMpx.PrimaryTx.Commit()
	if err != nil {
		return txMpx.PrimaryTx.EventErr("dbr.primary.commit.error", err), nil
	}
	txMpx.PrimaryTx.Event("dbr.primary.commit")

	secondaryErrChan := make(chan error)

	eg, egCtx := errgroup.WithContext(context.Background())

	go func() {
		defer close(secondaryErrChan)
		secondaryErrChan <- eg.Wait()
	}()

	eg.Go(func() error {
		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			default:
				rerr := txMpx.SecondaryTx.Commit()
				if rerr != nil {
					return txMpx.SecondaryTx.EventErr("dbr.secondary.commit.error", rerr)
				}
				txMpx.SecondaryTx.Event("dbr.secondary.commit")
				return nil
			}
		}
	})

	return nil, secondaryErrChan
}

// Rollback cancels the primary transaction and returns an error channel for the secondary transaction canceled asynchronously.
func (txMpx *TxMpx) Rollback() (error, chan error) {
	err := txMpx.PrimaryTx.Rollback()
	if err != nil {
		return txMpx.PrimaryTx.EventErr("dbr.primary.rollback", err), nil
	}
	txMpx.PrimaryTx.Event("dbr.primary.rollback")

	secondaryErrChan := make(chan error)

	eg, egCtx := errgroup.WithContext(context.Background())

	go func() {
		defer close(secondaryErrChan)
		secondaryErrChan <- eg.Wait()
	}()

	eg.Go(func() error {
		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			default:
				rerr := txMpx.SecondaryTx.Rollback()
				if rerr != nil {
					return txMpx.SecondaryTx.EventErr("dbr.secondary.rollback", rerr)
				}
				txMpx.SecondaryTx.Event("dbr.secondary.rollback")
				return nil
			}
		}
	})

	return nil, secondaryErrChan
}

// RollbackUnlessCommitted rollsback the primary transaction unless
// it has already been committed or rolled back. It also asychronously rolls
// back the secondary transaction unless it has been committed or rolled back.
//
// Useful to defer tx.RollbackUnlessCommitted(), so you don't
// have to handle N failure cases.
// Keep in mind the only way to detect an error on the rollback
// is via the event log.
func (txMpx *TxMpx) RollbackUnlessCommitted() {
	err := txMpx.PrimaryTx.Rollback()
	if err == sql.ErrTxDone {
		// ok
	} else if err != nil {
		txMpx.PrimaryTx.EventErr("dbr.primary.rollback_unless_committed", err)
	} else {
		txMpx.PrimaryTx.Event("dbr.primary.rollback")
	}

	go func() {
		rerr := txMpx.SecondaryTx.Rollback()
		if rerr == sql.ErrTxDone {
			// ok
		} else if rerr != nil {
			txMpx.SecondaryTx.EventErr("dbr.secondary.rollback_unless_committed", rerr)
		} else {
			txMpx.SecondaryTx.Event("dbr.secondary.rollback")
		}
	}()
}
