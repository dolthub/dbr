// Package dbr provides additions to Go's database/sql for super fast performance and convenience.
package dbr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/gocraft/dbr/v2/dialect"
)

var errRowsAffectedNotEqual = errors.New("rows affected not equal")

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

// ConnectionMpx multiplexes two connections.
// It uses a Queue to execute statements against the secondary
// connection
type ConnectionMpx struct {
	shouldSyncAtCommit bool
	enableDoubleReads  bool
	PrimaryConn        *Connection
	SecondaryConn      *Connection
}

func NewConnectionMpxFromConnections(primaryConn *Connection, secondaryConn *Connection, shouldSyncAtCommit, enableDoubleReads bool) *ConnectionMpx {
	return &ConnectionMpx{
		shouldSyncAtCommit: shouldSyncAtCommit,
		enableDoubleReads:  enableDoubleReads,
		PrimaryConn:        primaryConn,
		SecondaryConn:      secondaryConn,
	}
}

func (connMpx *ConnectionMpx) Exec(query string, args ...interface{}) (sql.Result, error) {
	primaryRes, err := connMpx.PrimaryConn.Exec(query, args...)
	if err != nil {
		return nil, err
	}

	// naked go routine to match parity of Connection.Exec
	go func() {
		_, cerr := connMpx.SecondaryConn.Exec(query, args...)
		if cerr != nil {
			connMpx.SecondaryConn.EventReceiver.EventErr("dbr.secondary.connection.exec", cerr)
		}
	}()

	return primaryRes, err
}

func (connMpx *ConnectionMpx) Close() error {
	err := connMpx.PrimaryConn.Close()

	// naked go routine to match parity of Connection.Close
	go func() {
		cerr := connMpx.SecondaryConn.Close()
		if cerr != nil {
			connMpx.SecondaryConn.EventReceiver.EventErr("dbr.secondary.connection.close", cerr)
		}
	}()

	return err
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
	Timeout                time.Duration
	secondaryQ             *Queue
}

func (sessMpx *SessionMpx) PrimaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return sessMpx.PrimaryConn.ExecContext(ctx, query, args...)
}

func (sessMpx *SessionMpx) SecondaryExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return sessMpx.SecondaryConn.ExecContext(ctx, query, args...)
}

func (sessMpx *SessionMpx) PrimaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return sessMpx.PrimaryConn.QueryContext(ctx, query, args...)
}

func (sessMpx *SessionMpx) SecondaryQueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return sessMpx.SecondaryConn.QueryContext(ctx, query, args...)
}

// AddJob adds a job to the secondary queue.
func (sessMpx *SessionMpx) AddJob(job *Job) error {
	return sessMpx.secondaryQ.AddJob(job)
}

// Wait waits for the secondary queue to finish all work.
func (sessMpx *SessionMpx) Wait() error {
	return sessMpx.secondaryQ.Wait()
}

// Close closes the primary and secondary connections and waits for the secondary queue to finish all work.
func (sessMpx *SessionMpx) Close() error {
	j := NewJob("dbr.secondary.close", nil, func() error {
		return sessMpx.SecondaryConn.Close()
	})

	err := sessMpx.secondaryQ.AddJobAndClose(j)
	if err != nil {
		return err
	}

	err = sessMpx.PrimaryConn.Close()
	if err != nil {
		return err
	}

	return sessMpx.secondaryQ.Wait()
}

func (sessMpx *SessionMpx) SetSecondaryEventReceiver(log EventReceiver) {
	sessMpx.secondaryQ.SetEventReceiver(log)
	sessMpx.SecondaryEventReceiver = log
}

func (sessMpx *SessionMpx) GetTimeout() time.Duration {
	return sessMpx.Timeout
}

func (sessMpx *SessionMpx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return sessMpx.ExecContext(context.Background(), query, args...)
}

func (sessMpx *SessionMpx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	primaryRes, err := sessMpx.PrimaryExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	j := NewJob("dbr.secondary.exec_context", map[string]string{"sql": query}, func() error {
		_, err := sessMpx.SecondaryExecContext(NewContextWithMetricValues(ctx), query, args...)
		return err
	})
	err = sessMpx.AddJob(j)
	return primaryRes, err
}

func (sessMpx *SessionMpx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return sessMpx.PrimaryQueryContext(ctx, query, args...)
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
func (connMpx *ConnectionMpx) NewSessionMpx(primaryLog, secondaryLog EventReceiver) *SessionMpx {
	if primaryLog == nil {
		primaryLog = connMpx.PrimaryConn.EventReceiver // Use parent instrumentation
	}
	if secondaryLog == nil {
		secondaryLog = connMpx.SecondaryConn.EventReceiver
	}

	q := NewWorkingQueue(context.Background(), 500, secondaryLog)
	return &SessionMpx{
		ConnectionMpx:          connMpx,
		PrimaryEventReceiver:   primaryLog,
		SecondaryEventReceiver: secondaryLog,
		secondaryQ:             q,
	}
}

// Ensure that tx and session are session runner
// ensure the txmpx and sessionmpx are runnermpx
var (
	_ SessionRunner = (*Tx)(nil)
	_ SessionRunner = (*Session)(nil)
	_ SessionRunner = (*TxMpx)(nil)
	_ SessionRunner = (*SessionMpx)(nil)
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

type runner interface {
	GetTimeout() time.Duration
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type RunnerMpx interface {
	AddJob(job *Job) error
	GetTimeout() time.Duration
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

func execMpx(
	ctx context.Context,
	runnerMpx RunnerMpx,
	primaryLog,
	secondaryLog EventReceiver,
	builder Builder,
	primaryD,
	secondaryD Dialect) (sql.Result, string, error) {

	timeout := runnerMpx.GetTimeout()

	basePrimaryCtx := ctx

	var primaryCtx context.Context
	if timeout > 0 {
		var cancel func()
		primaryCtx, cancel = context.WithTimeout(basePrimaryCtx, timeout)
		defer cancel()
	} else {
		primaryCtx = basePrimaryCtx
	}

	primaryI := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      primaryD,
		IgnoreBinary: true,
	}

	err := primaryI.encodePlaceholder(builder, true)
	primaryQuery, primaryValue := primaryI.String(), primaryI.Value()
	if err != nil {
		return nil, primaryQuery, primaryLog.EventErrKv("dbr.primary.exec.interpolate", err, kvs{
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
			primaryTraceImpl.SpanError(primaryCtx, err)
		}
		return primaryResults, primaryQuery, primaryLog.EventErrKv("dbr.primary.exec.exec", err, kvs{
			"sql": primaryQuery,
		})
	}

	baseSecondaryCtx := NewContextWithMetricValues(ctx)

	j := &Job{
		exec: func() error {
			var secondaryCtx context.Context
			if timeout > 0 {
				var cancel func()
				secondaryCtx, cancel = context.WithTimeout(baseSecondaryCtx, timeout)
				defer cancel()
			} else {
				secondaryCtx = baseSecondaryCtx
			}

			secondaryI := interpolator{
				Buffer:       NewBuffer(),
				Dialect:      secondaryD,
				IgnoreBinary: true,
			}

			rerr := secondaryI.encodePlaceholder(builder, true)
			secondaryQuery, secondaryValue := secondaryI.String(), secondaryI.Value()
			if rerr != nil {
				return secondaryLog.EventErrKv("dbr.secondary.exec.interpolate", rerr, kvs{
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

			secondaryResults, rerr := runnerMpx.SecondaryExecContext(secondaryCtx, secondaryQuery, secondaryValue...)
			if rerr != nil {
				if secondaryHasTracingImpl {
					secondaryTraceImpl.SpanError(secondaryCtx, rerr)
				}
				return secondaryLog.EventErrKv("dbr.secondary.exec.exec", rerr, kvs{
					"sql": secondaryQuery,
				})
			}

			// assert primary and secondary are same
			primaryRowsAffected, rerr := primaryResults.RowsAffected()
			if rerr != nil {
				return secondaryLog.EventErr("dbr.secondary.primary.assertion.error", rerr)
			}

			secondaryRowsAffected, rerr := secondaryResults.RowsAffected()
			if rerr != nil {
				return secondaryLog.EventErr("dbr.secondary.primary.assertion.error", rerr)
			}

			if secondaryRowsAffected != primaryRowsAffected {
				if secondaryHasTracingImpl {
					secondaryTraceImpl.SpanError(secondaryCtx, errRowsAffectedNotEqual)
				}
				return secondaryLog.EventErrKv("dbr.secondary.primary.assertion.error", errRowsAffectedNotEqual, kvs{
					"primarySql":            primaryQuery,
					"primaryRowsAffected":   strconv.FormatInt(primaryRowsAffected, 10),
					"secondarySql":          secondaryQuery,
					"secondaryRowsAffected": strconv.FormatInt(secondaryRowsAffected, 10),
				})
			}
			return nil
		},
	}
	err = runnerMpx.AddJob(j)
	if err != nil {
		return nil, "", err
	}

	return primaryResults, primaryQuery, nil
}

func queryRows(ctx context.Context, runner runner, log EventReceiver, builder Builder, d Dialect) (string, *sql.Rows, error) {
	// discard the timeout set in the runner, the context should not be canceled
	// implicitly here but explicitly by the caller since the returned *sql.Rows
	// may still be listening to the context
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

func queryRowsMpx(ctx context.Context, runnerMpx RunnerMpx, primaryLog, secondaryLog EventReceiver, builder Builder, primaryD, secondaryD Dialect) (string, *sql.Rows, error) {
	primaryCtx := ctx

	// discard the timeout set in the runner, the context should not be canceled
	// implicitly here but explicitly by the caller since the returned *sql.Rows
	// may still be listening to the context
	primaryI := interpolator{
		Buffer:       NewBuffer(),
		Dialect:      primaryD,
		IgnoreBinary: true,
	}
	err := primaryI.encodePlaceholder(builder, true)
	primaryQuery, primaryValue := primaryI.String(), primaryI.Value()
	if err != nil {
		return primaryQuery, nil, primaryLog.EventErrKv("dbr.primary.select.interpolate", err, kvs{
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
		primaryCtx = primaryTraceImpl.SpanStart(primaryCtx, "dbr.primary.select", primaryQuery)
		defer primaryTraceImpl.SpanFinish(primaryCtx)
	}

	primaryRows, err := runnerMpx.PrimaryQueryContext(primaryCtx, primaryQuery, primaryValue...)
	if err != nil {
		if primaryHasTracingImpl {
			primaryTraceImpl.SpanError(primaryCtx, err)
		}
		return primaryQuery, nil, primaryLog.EventErrKv("dbr.primary.select.load.query", err, kvs{
			"sql": primaryQuery,
		})
	}

	secondaryCtx := NewContextWithMetricValues(ctx)
	j := &Job{
		exec: func() error {
			secondaryI := interpolator{
				Buffer:       NewBuffer(),
				Dialect:      secondaryD,
				IgnoreBinary: true,
			}
			rerr := secondaryI.encodePlaceholder(builder, true)
			secondaryQuery, secondaryValue := secondaryI.String(), secondaryI.Value()
			if rerr != nil {
				return secondaryLog.EventErrKv("dbr.secondary.select.interpolate", rerr, kvs{
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
				secondaryCtx = secondaryTraceImpl.SpanStart(secondaryCtx, "dbr.secondary.select", secondaryQuery)
				defer secondaryTraceImpl.SpanFinish(secondaryCtx)
			}

			_, rerr = runnerMpx.SecondaryQueryContext(secondaryCtx, secondaryQuery, secondaryValue...)
			if rerr != nil {
				if secondaryHasTracingImpl {
					secondaryTraceImpl.SpanError(secondaryCtx, rerr)
				}
				return secondaryLog.EventErrKv("dbr.secondary.select.load.query", rerr, kvs{
					"sql": secondaryQuery,
				})
			}

			return nil
		},
	}
	err = runnerMpx.AddJob(j)
	if err != nil {
		return "", nil, err
	}

	return primaryQuery, primaryRows, nil
}

func queryMpx(ctx context.Context, runnerMpx RunnerMpx, primaryLog, secondaryLog EventReceiver, builder Builder, primaryD, secondaryD Dialect, primaryDest interface{}) (int, string, error) {
	timeout := runnerMpx.GetTimeout()
	if timeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	query, rows, err := queryRowsMpx(ctx, runnerMpx, primaryLog, secondaryLog, builder, primaryD, secondaryD)
	count, err := Load(rows, primaryDest)
	if err != nil {
		return 0, "", err
	}

	return count, query, err
}
