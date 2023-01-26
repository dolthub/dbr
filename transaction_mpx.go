package dbr

import (
	"context"
	"database/sql"
	"time"
)

// TxMpx is a multiplexed transaction created by SessionMpx.
type TxMpx struct {
	PrimaryTx   *Tx
	SecondaryTx *Tx
	SecondaryQ  *Queue
}

func (txMpx *TxMpx) AddJob(job *Job) {
	txMpx.SecondaryQ.AddJob(job)
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

// BeginTxs creates a transaction with TxOptions.
func (smpx *SessionMpx) BeginTxs(ctx context.Context, opts *sql.TxOptions) (*TxMpx, error) {
	primaryTx, err := smpx.PrimaryConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, smpx.PrimaryConn.EventErr("dbr.primary.begin.error", err)
	}
	smpx.PrimaryConn.Event("dbr.primary.begin")

	// TODO: not sure how to make this nonblocking for primary txs
	secondaryTx, err := smpx.SecondaryConn.BeginTx(ctx, opts)
	if err != nil {
		return nil, smpx.SecondaryConn.EventErr("dbr.secondary.begin.error", err)
	}
	smpx.SecondaryConn.Event("dbr.secondary.begin")

	to := smpx.GetTimeout()
	return &TxMpx{
		PrimaryTx: &Tx{
			EventReceiver: smpx.PrimaryEventReceiver,
			Dialect:       smpx.PrimaryConn.Dialect,
			Tx:            primaryTx,
			Timeout:       to,
		},
		SecondaryTx: &Tx{
			EventReceiver: smpx.SecondaryEventReceiver,
			Dialect:       smpx.SecondaryConn.Dialect,
			Tx:            secondaryTx,
			Timeout:       to,
		},
		SecondaryQ: smpx.secondaryQ,
	}, nil
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
	j := &Job{Exec: func() error {
		_, err := txMpx.SecondaryTx.ExecContext(ctx, query, args...)
		return err
	}}
	txMpx.SecondaryQ.AddJob(j)
	return txMpx.PrimaryTx.ExecContext(ctx, query, args...)
}

func (txMpx *TxMpx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	j := &Job{Exec: func() error {
		_, err := txMpx.SecondaryTx.QueryContext(ctx, query, args...)
		return err
	}}
	txMpx.SecondaryQ.AddJob(j)
	return txMpx.PrimaryTx.QueryContext(ctx, query, args...)
}

// Commit finishes the transaction.
func (txMpx *TxMpx) Commit() error {
	j := &Job{Exec: func() error {
		err := txMpx.SecondaryTx.Commit()
		if err != nil {
			return txMpx.SecondaryTx.EventErr("dbr.secondary.commit.error", err)
		}
		txMpx.SecondaryTx.Event("dbr.secondary.commit")
		return nil
	}}
	txMpx.SecondaryQ.AddJob(j)

	err := txMpx.PrimaryTx.Commit()
	if err != nil {
		return txMpx.PrimaryTx.EventErr("dbr.primary.commit.error", err)
	}
	txMpx.PrimaryTx.Event("dbr.primary.commit")
	return nil
}

// Rollback cancels the transaction.
func (txMpx *TxMpx) Rollback() error {
	j := &Job{Exec: func() error {
		err := txMpx.SecondaryTx.Rollback()
		if err != nil {
			return txMpx.SecondaryTx.EventErr("dbr.secondary.rollback", err)
		}
		txMpx.SecondaryTx.Event("dbr.secondary.rollback")
		return nil
	}}
	txMpx.SecondaryQ.AddJob(j)

	err := txMpx.PrimaryTx.Rollback()
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
	j := &Job{Exec: func() error {
		err := txMpx.SecondaryTx.Rollback()
		if err == sql.ErrTxDone {
			// ok
		} else if err != nil {
			txMpx.SecondaryTx.EventErr("dbr.secondary.rollback_unless_committed", err)
		} else {
			txMpx.SecondaryTx.Event("dbr.secondary.rollback")
		}
		return nil
	}}
	txMpx.SecondaryQ.AddJob(j)

	err := txMpx.PrimaryTx.Rollback()
	if err == sql.ErrTxDone {
		// ok
	} else if err != nil {
		txMpx.PrimaryTx.EventErr("dbr.primary.rollback_unless_committed", err)
	} else {
		txMpx.PrimaryTx.Event("dbr.primary.rollback")
	}
}
