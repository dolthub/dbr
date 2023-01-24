// Package dbr provides additions to Go's database/sql for super fast performance and convenience.
package dbr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"time"

	"github.com/gocraft/dbr/v2/dialect"
)

// Open creates a Connection.
// log can be nil to ignore logging.
func Open(driver, dsn string, log EventReceiver) (*Connection, error) {
	if log == nil {
		log = nullReceiver
	}
	conn, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	var d Dialect
	switch driver {
	case "mysql":
		d = dialect.MySQL
	case "postgres", "pgx":
		d = dialect.PostgreSQL
	case "sqlite3":
		d = dialect.SQLite3
	case "mssql":
		d = dialect.MSSQL
	default:
		return nil, ErrNotSupported
	}
	return &Connection{DB: conn, EventReceiver: log, Dialect: d}, nil
}

// OpenMpx creates a ConnectionMpx.
// log can be nil to ignore logging.
func OpenMpx(primaryDriver, primaryDsn, secondaryDriver, secondaryDsn string, primaryLog, secondaryLog EventReceiver) (*ConnectionMpx, error) {
	primary, err := Open(primaryDriver, primaryDsn, primaryLog)
	if err != nil {
		return nil, err
	}
	secondary, err := Open(secondaryDriver, secondaryDsn, secondaryLog)
	if err != nil {
		return nil, err
	}
	return &ConnectionMpx{
		PrimaryConn:   primary,
		SecondaryConn: secondary,
	}, nil
}

const (
	placeholder = "?"
)

// Connection wraps sql.DB with an EventReceiver
// to send events, errors, and timings.
type Connection struct {
	*sql.DB
	Dialect
	EventReceiver
}

// ConnectionMpx multiplexes two connections
type ConnectionMpx struct {
	PrimaryConn   *Connection
	SecondaryConn *Connection
}

// Session represents a business unit of execution.
//
// All queries in gocraft/dbr are made in the context of a session.
// This is because when instrumenting your app, it's important
// to understand which business action the query took place in.
//
// A custom EventReceiver can be set.
//
// Timeout specifies max duration for an operation like Select.
type Session struct {
	*Connection
	EventReceiver
	Timeout time.Duration
}

// SessionMpx contains a ConnectionMpx
type SessionMpx struct {
	*ConnectionMpx
	PrimaryEventReceiver   EventReceiver
	SecondaryEventReceiver EventReceiver
	PrimaryTimeout         time.Duration
	SecondaryTimeout       time.Duration
}

// GetTimeout, ExecContext, and QueryContext are required
// for reads, which are not multiplexed at this time.

// GetTimeout returns the primary connection's timeout
func (sessMpx *SessionMpx) GetTimeout() time.Duration {
	return sessMpx.GetPrimaryTimeout()
}

// ExecContext calls ExecContext on the primary connection
func (sessMpx *SessionMpx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return sessMpx.PrimaryConn.ExecContext(ctx, query, args...)
}

// QueryContext calls QueryContext on the primary connection
func (sessMpx *SessionMpx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return sessMpx.PrimaryConn.QueryContext(ctx, query, args...)
}

func (smpx *SessionMpx) GetPrimaryTimeout() time.Duration {
	return smpx.PrimaryTimeout
}

func (smpx *SessionMpx) GetSecondaryTimeout() time.Duration {
	return smpx.SecondaryTimeout
}

func (smpx *SessionMpx) PrimaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return smpx.PrimaryConn.ExecContext(ctx, query, args...)
}

func (smpx *SessionMpx) SecondaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return smpx.SecondaryConn.ExecContext(ctx, query, args...)
}

func (smpx *SessionMpx) PrimaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return smpx.PrimaryConn.QueryContext(ctx, query, args...)
}

func (smpx *SessionMpx) SecondaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return smpx.SecondaryConn.QueryContext(ctx, query, args...)
}

// GetTimeout returns current timeout enforced in session.
func (sess *Session) GetTimeout() time.Duration {
	return sess.Timeout
}

// NewSession instantiates a Session from Connection.
// If log is nil, Connection EventReceiver is used.
func (conn *Connection) NewSession(log EventReceiver) *Session {
	if log == nil {
		log = conn.EventReceiver // Use parent instrumentation
	}
	return &Session{Connection: conn, EventReceiver: log}
}

// NewSessionMpx instantiates a SessionMpx from ConnectionMpx.
// If log is nil, ConnectionMpx's EventReceivers are used.
func (cmpx *ConnectionMpx) NewSessionMpx(primaryLog, secondaryLog EventReceiver) *SessionMpx {
	if primaryLog == nil {
		primaryLog = cmpx.PrimaryConn.EventReceiver // Use parent instrumentation
	}
	if secondaryLog == nil {
		secondaryLog = cmpx.SecondaryConn.EventReceiver
	}
	return &SessionMpx{ConnectionMpx: cmpx, PrimaryEventReceiver: primaryLog, SecondaryEventReceiver: secondaryLog}
}

// Ensure that tx and session are session runner
// ensure the txmpx and sessionmpx are runnermpx
var (
	_ SessionRunner    = (*Tx)(nil)
	_ SessionRunner    = (*Session)(nil)
	_ SessionRunnerMpx = (*TxMpx)(nil)
	_ SessionRunnerMpx = (*SessionMpx)(nil)
)

// SessionRunner can do anything that a Session can except start a transaction.
// Both Session and Tx implements this interface.
type SessionRunner interface {
	Select(column ...string) *SelectBuilder
	SelectBySql(query string, value ...interface{}) *SelectBuilder

	InsertInto(table string) *InsertBuilder
	InsertBySql(query string, value ...interface{}) *InsertBuilder

	Update(table string) *UpdateBuilder
	UpdateBySql(query string, value ...interface{}) *UpdateBuilder

	DeleteFrom(table string) *DeleteBuilder
	DeleteBySql(query string, value ...interface{}) *DeleteBuilder
}

// SessionRunnerMpx can do anything that a Session can except start a transaction.
// Both SessionMpx and TxMpx implements this interface.
type SessionRunnerMpx interface {
	Select(column ...string) *SelectBuilder
	SelectBySql(query string, value ...interface{}) *SelectBuilder

	InsertInto(table string) *InsertBuilderMpx
	InsertBySql(query string, value ...interface{}) *InsertBuilderMpx

	Update(table string) *UpdateBuilderMpx
	UpdateBySql(query string, value ...interface{}) *UpdateBuilderMpx

	DeleteFrom(table string) *DeleteBuilderMpx
	DeleteBySql(query string, value ...interface{}) *DeleteBuilderMpx
}

type runner interface {
	GetTimeout() time.Duration
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type RunnerMpx interface {
	GetPrimaryTimeout() time.Duration
	GetSecondaryTimeout() time.Duration
	PrimaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	SecondaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	PrimaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	SecondaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func exec(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect) (sql.Result, string, error) {
	timeout := runner.GetTimeout()
	if timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	i := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      d,
		IgnoreBinary: true,
	}
	err := i.encodePlaceholder(builder, true)
	query, value := i.String(), i.Value()
	if err != nil {
		return nil, query, log.EventErrKv("dbr.exec.interpolate", err, kvs{
			"sql":  query,
			"args": fmt.Sprint(value),
		})
	}

	startTime := time.Now()
	defer func() {
		log.TimingKv("dbr.exec", time.Since(startTime).Nanoseconds(), kvs{
			"sql": query,
		})
	}()

	traceImpl, hasTracingImpl := log.(TracingEventReceiver)
	if hasTracingImpl {
		ctx = traceImpl.SpanStart(ctx, "dbr.exec", query)
		defer traceImpl.SpanFinish(ctx)
	}

	result, err := runner.ExecContext(ctx, query, value...)
	if err != nil {
		if hasTracingImpl {
			traceImpl.SpanError(ctx, err)
		}
		return result, query, log.EventErrKv("dbr.exec.exec", err, kvs{
			"sql": query,
		})
	}
	return result, query, nil
}

type AsyncResult struct {
	Query   string
	ErrChan chan error
	Result  sql.Result
}

type AsyncRows struct {
	Query   string
	ErrChan chan error
	Rows    *sql.Rows
}

type AsyncCount struct {
	Query   string
	ErrChan chan error
	Count   int
}

type AsyncResultChan chan *AsyncResult
type AsyncRowsChan chan *AsyncRows
type AsyncCountChan chan *AsyncCount

func execMpx(
	ctx context.Context,
	runnerMpx RunnerMpx,
	primaryLog,
	secondaryLog EventReceiver,
	builder Builder,
	primaryD,
	secondaryD Dialect) (sql.Result, string, AsyncResultChan, error) {
	primaryTimeout := runnerMpx.GetPrimaryTimeout()

	baseCtx := ctx

	var primaryCtx context.Context
	if primaryTimeout > 0 {
		var cancel func()
		primaryCtx, cancel = context.WithTimeout(baseCtx, primaryTimeout)
		defer cancel()
	} else {
		primaryCtx = baseCtx
	}

	primaryI := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      primaryD,
		IgnoreBinary: true,
	}

	err := primaryI.encodePlaceholder(builder, true)
	primaryQuery, primaryValue := primaryI.String(), primaryI.Value()
	if err != nil {
		return nil, primaryQuery, nil, primaryLog.EventErrKv("dbr.primary.exec.interpolate", err, kvs{
			"sql":  primaryQuery,
			"args": fmt.Sprint(primaryValue),
		})
	}

	primaryStartTime := time.Now()
	defer func() {
		primaryLog.TimingKv("dbr.primary.exec", time.Since(primaryStartTime).Nanoseconds(), kvs{
			"sql": primaryQuery,
		})
	}()

	primaryTraceImpl, primaryHasTracingImpl := primaryLog.(TracingEventReceiver)
	if primaryHasTracingImpl {
		primaryCtx = primaryTraceImpl.SpanStart(primaryCtx, "dbr.primary.exec", primaryQuery)
		defer primaryTraceImpl.SpanFinish(primaryCtx)
	}

	primaryResults, err := runnerMpx.PrimaryExecContext(primaryCtx, primaryQuery, primaryValue...)
	if err != nil {
		if primaryHasTracingImpl {
			primaryTraceImpl.SpanError(ctx, err)
		}
		return primaryResults, primaryQuery, nil, primaryLog.EventErrKv("dbr.primary.exec.exec", err, kvs{
			"sql": primaryQuery,
		})
	}

	secondaryErrChan := make(chan error)

	secondaryAsyncResultChan := make(AsyncResultChan)
	eg, egCtx := errgroup.WithContext(baseCtx)

	go func() {
		defer close(secondaryErrChan)
		secondaryErrChan <- eg.Wait()
	}()

	eg.Go(func() error {
		defer close(secondaryAsyncResultChan)

		secondaryTimeout := runnerMpx.GetSecondaryTimeout()

		var secondaryCtx context.Context
		if secondaryTimeout > 0 {
			var cancel func()
			secondaryCtx, cancel = context.WithTimeout(egCtx, secondaryTimeout)
			defer cancel()
		} else {
			secondaryCtx = egCtx
		}

		secondaryI := interpolator{
			Buffer:       NewBuffer(),
			Dialect:      secondaryD,
			IgnoreBinary: true,
		}

		serr := secondaryI.encodePlaceholder(builder, true)
		secondaryQuery, secondaryValue := secondaryI.String(), secondaryI.Value()
		if serr != nil {

			secondaryAsyncResultChan <- &AsyncResult{ErrChan: secondaryErrChan}
			return secondaryLog.EventErrKv("dbr.secondary.exec.interpolate", serr, kvs{
				"sql":  secondaryQuery,
				"args": fmt.Sprint(secondaryValue),
			})
		}

		secondaryStartTime := time.Now()
		defer func() {
			secondaryLog.TimingKv("dbr.secondary.exec", time.Since(secondaryStartTime).Nanoseconds(), kvs{
				"sql": secondaryQuery,
			})
		}()

		secondaryTraceImpl, secondaryHasTracingImpl := secondaryLog.(TracingEventReceiver)
		if secondaryHasTracingImpl {
			secondaryCtx = secondaryTraceImpl.SpanStart(secondaryCtx, "dbr.secondary.exec", secondaryQuery)
			defer secondaryTraceImpl.SpanFinish(secondaryCtx)
		}

		secondaryResults, serr := runnerMpx.SecondaryExecContext(secondaryCtx, secondaryQuery, secondaryValue...)
		if serr != nil {
			if secondaryHasTracingImpl {
				secondaryTraceImpl.SpanError(ctx, serr)
			}

			secondaryAsyncResultChan <- &AsyncResult{ErrChan: secondaryErrChan}
			return secondaryLog.EventErrKv("dbr.secondary.exec.exec", serr, kvs{
				"sql": secondaryQuery,
			})
		}

		ar := &AsyncResult{
			Result:  secondaryResults,
			Query:   secondaryQuery,
			ErrChan: secondaryErrChan,
		}

		secondaryAsyncResultChan <- ar
		return nil
	})

	return primaryResults, primaryQuery, secondaryAsyncResultChan, nil
}

func queryRows(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect) (string, *sql.Rows, error) {
	// discard the timeout set in the runner, the context should not be canceled
	// implicitly here but explicitly by the caller since the returned *sql.Rows
	// may still listening to the context
	i := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      d,
		IgnoreBinary: true,
	}
	err := i.encodePlaceholder(builder, true)
	query, value := i.String(), i.Value()
	if err != nil {
		return query, nil, log.EventErrKv("dbr.select.interpolate", err, kvs{
			"sql":  query,
			"args": fmt.Sprint(value),
		})
	}

	startTime := time.Now()
	defer func() {
		log.TimingKv("dbr.select", time.Since(startTime).Nanoseconds(), kvs{
			"sql": query,
		})
	}()

	traceImpl, hasTracingImpl := log.(TracingEventReceiver)
	if hasTracingImpl {
		ctx = traceImpl.SpanStart(ctx, "dbr.select", query)
		defer traceImpl.SpanFinish(ctx)
	}

	rows, err := runner.QueryContext(ctx, query, value...)
	if err != nil {
		if hasTracingImpl {
			traceImpl.SpanError(ctx, err)
		}
		return query, nil, log.EventErrKv("dbr.select.load.query", err, kvs{
			"sql": query,
		})
	}

	return query, rows, nil
}

func query(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect, dest interface{}) (int, string, error) {
	timeout := runner.GetTimeout()
	if timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	query, rows, err := queryRows(ctx, runner, log, builder, d)
	if err != nil {
		return 0, query, err
	}
	count, err := Load(rows, dest)
	if err != nil {
		return 0, query, log.EventErrKv("dbr.select.load.scan", err, kvs{
			"sql": query,
		})
	}
	return count, query, nil
}

func queryRowsMpx(ctx context.Context, runnerMpx RunnerMpx, primaryLog, secondaryLog EventReceiver, builder Builder, primaryD, secondaryD Dialect) (string, *sql.Rows, AsyncRowsChan, error) {
	// discard the timeout set in the runner, the context should not be canceled
	// implicitly here but explicitly by the caller since the returned *sql.Rows
	// may still listening to the context
	primaryI := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      primaryD,
		IgnoreBinary: true,
	}
	err := primaryI.encodePlaceholder(builder, true)
	primaryQuery, primaryValue := primaryI.String(), primaryI.Value()
	if err != nil {
		return primaryQuery, nil, nil, primaryLog.EventErrKv("dbr.primary.select.interpolate", err, kvs{
			"sql":  primaryQuery,
			"args": fmt.Sprint(primaryValue),
		})
	}

	primaryStartTime := time.Now()
	defer func() {
		primaryLog.TimingKv("dbr.primary.select", time.Since(primaryStartTime).Nanoseconds(), kvs{
			"sql": primaryQuery,
		})
	}()

	primaryTraceImpl, primaryHasTracingImpl := primaryLog.(TracingEventReceiver)
	if primaryHasTracingImpl {
		ctx = primaryTraceImpl.SpanStart(ctx, "dbr.primary.select", primaryQuery)
		defer primaryTraceImpl.SpanFinish(ctx)
	}

	primaryRows, err := runnerMpx.PrimaryQueryContext(ctx, primaryQuery, primaryValue...)
	if err != nil {
		if primaryHasTracingImpl {
			primaryTraceImpl.SpanError(ctx, err)
		}
		return primaryQuery, nil, nil, primaryLog.EventErrKv("dbr.primary.select.load.query", err, kvs{
			"sql": primaryQuery,
		})
	}

	secondaryErrChan := make(chan error)

	secondaryAsyncRowsChan := make(AsyncRowsChan)
	eg, egCtx := errgroup.WithContext(ctx)

	go func() {
		defer close(secondaryErrChan)
		secondaryErrChan <- eg.Wait()
	}()

	eg.Go(func() error {
		secondaryI := interpolator{
			Buffer:       NewBuffer(),
			Dialect:      secondaryD,
			IgnoreBinary: true,
		}

		serr := secondaryI.encodePlaceholder(builder, true)
		secondaryQuery, secondaryValue := secondaryI.String(), secondaryI.Value()
		if serr != nil {

			secondaryAsyncRowsChan <- &AsyncRows{ErrChan: secondaryErrChan}
			return secondaryLog.EventErrKv("dbr.secondary.select.interpolate", serr, kvs{
				"sql":  secondaryQuery,
				"args": fmt.Sprint(secondaryValue),
			})
		}

		secondaryStartTime := time.Now()
		defer func() {
			secondaryLog.TimingKv("dbr.secondary.select", time.Since(secondaryStartTime).Nanoseconds(), kvs{
				"sql": secondaryQuery,
			})
		}()

		secondaryTraceImpl, secondaryHasTracingImpl := secondaryLog.(TracingEventReceiver)
		if secondaryHasTracingImpl {
			ctx = secondaryTraceImpl.SpanStart(egCtx, "dbr.secondary.select", secondaryQuery)
			defer secondaryTraceImpl.SpanFinish(egCtx)
		}

		secondaryRows, serr := runnerMpx.SecondaryQueryContext(egCtx, secondaryQuery, secondaryValue...)
		if serr != nil {
			if secondaryHasTracingImpl {
				secondaryTraceImpl.SpanError(egCtx, serr)
			}

			secondaryAsyncRowsChan <- &AsyncRows{ErrChan: secondaryErrChan}
			return secondaryLog.EventErrKv("dbr.secondary.select.load.query", serr, kvs{
				"sql": secondaryQuery,
			})
		}

		ar := &AsyncRows{
			Rows:    secondaryRows,
			Query:   secondaryQuery,
			ErrChan: secondaryErrChan,
		}

		secondaryAsyncRowsChan <- ar
		return nil
	})

	return primaryQuery, primaryRows, secondaryAsyncRowsChan, nil
}

func queryMpx(ctx context.Context, runnerMpx RunnerMpx, primaryLog, secondaryLog EventReceiver, builder Builder, primaryD, secondaryD Dialect, primaryDest, secondaryDest interface{}) (int, string, AsyncCountChan, error) {
	primaryTimeout := runnerMpx.GetPrimaryTimeout()
	if primaryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, primaryTimeout)
		defer cancel()
	}

	secondaryTimeout := runnerMpx.GetSecondaryTimeout()
	if secondaryTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, secondaryTimeout)
		defer cancel()
	}

	primaryQuery, primaryRows, secondaryAsyncRowsChan, err := queryRowsMpx(ctx, runnerMpx, primaryLog, secondaryLog, builder, primaryD, secondaryD)
	if err != nil {
		return 0, primaryQuery, nil, err
	}
	primaryCount, err := Load(primaryRows, primaryDest)
	if err != nil {
		return 0, primaryQuery, nil, primaryLog.EventErrKv("dbr.primary.select.load.scan", err, kvs{
			"sql": primaryQuery,
		})
	}

	secondaryErrChan := make(chan error)

	secondaryAsyncCountChan := make(AsyncCountChan)
	eg, egCtx := errgroup.WithContext(ctx)

	go func() {
		defer close(secondaryErrChan)
		secondaryErrChan <- eg.Wait()
	}()

	eg.Go(func() error {
		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case asyncRow, ok := <-secondaryAsyncRowsChan:
				if !ok {
					secondaryErrChan <- errors.New("failed to read secondary asyncRow")
				}

				for {
					select {
					case <-egCtx.Done():
						return egCtx.Err()
					case rerr, ok := <-asyncRow.ErrChan:
						if !ok {
							secondaryErrChan <- errors.New("failed to err secondary errChan")
						}
						if rerr != nil {
							return rerr
						}
					default:
						secondaryCount, rerr := Load(asyncRow.Rows, secondaryDest)
						if rerr != nil {
							return secondaryLog.EventErrKv("dbr.secondary.select.load.scan", rerr, kvs{
								"sql": asyncRow.Query,
							})
						}

						ac := &AsyncCount{
							Count:   secondaryCount,
							Query:   asyncRow.Query,
							ErrChan: secondaryErrChan,
						}

						secondaryAsyncCountChan <- ac
						return nil
					}
				}
			}
		}
	})

	return primaryCount, primaryQuery, secondaryAsyncCountChan, nil
}
