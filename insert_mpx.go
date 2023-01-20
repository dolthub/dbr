package dbr

import (
	"context"
	"database/sql"
	"errors"
	"golang.org/x/sync/errgroup"
	"reflect"
	"strings"

	"github.com/gocraft/dbr/v2/dialect"
)

// InsertStmtMpx builds `INSERT INTO ...`.
type InsertStmtMpx struct {
	RunnerMpx

	PrimaryEventReceiver   EventReceiver
	SecondaryEventReceiver EventReceiver

	PrimaryDialect   Dialect
	SecondaryDialect Dialect

	raw

	Table             string
	Column            []string
	Value             [][]interface{}
	Ignored           bool
	ReturnColumn      []string
	PrimaryRecordID   *int64
	SecondaryRecordID *int64
	comments          Comments
}

type InsertBuilderMpx = InsertStmtMpx

func (b *InsertStmtMpx) Build(d Dialect, buf Buffer) error {
	if b.raw.Query != "" {
		return b.raw.Build(d, buf)
	}

	if b.Table == "" {
		return ErrTableNotSpecified
	}

	if len(b.Column) == 0 {
		return ErrColumnNotSpecified
	}

	err := b.comments.Build(d, buf)
	if err != nil {
		return err
	}

	if b.Ignored {
		buf.WriteString("INSERT IGNORE INTO ")
	} else {
		buf.WriteString("INSERT INTO ")
	}

	buf.WriteString(d.QuoteIdent(b.Table))

	var placeholderBuf strings.Builder
	placeholderBuf.WriteString("(")
	buf.WriteString(" (")
	for i, col := range b.Column {
		if i > 0 {
			buf.WriteString(",")
			placeholderBuf.WriteString(",")
		}
		buf.WriteString(d.QuoteIdent(col))
		placeholderBuf.WriteString(placeholder)
	}
	buf.WriteString(")")

	if d == dialect.MSSQL && len(b.ReturnColumn) > 0 {
		buf.WriteString(" OUTPUT ")
		for i, col := range b.ReturnColumn {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString("INSERTED." + d.QuoteIdent(col))
		}
	}

	buf.WriteString(" VALUES ")
	placeholderBuf.WriteString(")")
	placeholderStr := placeholderBuf.String()

	for i, tuple := range b.Value {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(placeholderStr)

		buf.WriteValue(tuple...)
	}

	if d != dialect.MSSQL && len(b.ReturnColumn) > 0 {
		buf.WriteString(" RETURNING ")
		for i, col := range b.ReturnColumn {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(d.QuoteIdent(col))
		}
	}

	return nil
}

// InsertIntoMpx creates an InsertStmtMpx.
func InsertIntoMpx(table string) *InsertStmtMpx {
	return &InsertStmtMpx{
		Table: table,
	}
}

// InsertIntoMpx creates an InsertStmtMpx.
func (sessMpx *SessionMpx) InsertInto(table string) *InsertStmtMpx {
	b := InsertIntoMpx(table)
	b.RunnerMpx = sessMpx

	b.PrimaryEventReceiver = sessMpx.PrimaryEventReceiver
	b.SecondaryEventReceiver = sessMpx.SecondaryEventReceiver

	b.PrimaryDialect = sessMpx.PrimaryConn.Dialect
	b.SecondaryDialect = sessMpx.SecondaryConn.Dialect

	return b
}

// InsertIntoMpx creates an InsertStmtMpx.
func (txMpx *TxMpx) InsertInto(table string) *InsertStmtMpx {
	b := InsertIntoMpx(table)
	b.RunnerMpx = txMpx

	b.PrimaryEventReceiver = txMpx.PrimaryTx.EventReceiver
	b.SecondaryEventReceiver = txMpx.SecondaryTx.EventReceiver

	b.PrimaryDialect = txMpx.PrimaryTx.Dialect
	b.SecondaryDialect = txMpx.SecondaryTx.Dialect
	return b
}

// InsertBySqlMpx creates an InsertStmtMpx from raw query.
func InsertBySqlMpx(query string, value ...interface{}) *InsertStmtMpx {
	return &InsertStmtMpx{
		raw: raw{
			Query: query,
			Value: value,
		},
	}
}

// InsertBySqlMpx creates an InsertStmtMpx from raw query.
func (sessMpx *SessionMpx) InsertBySql(query string, value ...interface{}) *InsertStmtMpx {
	b := InsertBySqlMpx(query, value...)
	b.RunnerMpx = sessMpx

	b.PrimaryEventReceiver = sessMpx.PrimaryEventReceiver
	b.SecondaryEventReceiver = sessMpx.SecondaryEventReceiver

	b.PrimaryDialect = sessMpx.PrimaryConn.Dialect
	b.SecondaryDialect = sessMpx.SecondaryConn.Dialect

	return b
}

// InsertBySqlMpx creates an InsertStmtMpx from raw query.
func (txMpx *TxMpx) InsertBySql(query string, value ...interface{}) *InsertStmtMpx {
	b := InsertBySqlMpx(query, value...)

	b.RunnerMpx = txMpx

	b.PrimaryEventReceiver = txMpx.PrimaryTx.EventReceiver
	b.SecondaryEventReceiver = txMpx.SecondaryTx.EventReceiver

	b.PrimaryDialect = txMpx.PrimaryTx.Dialect
	b.SecondaryDialect = txMpx.SecondaryTx.Dialect

	return b
}

func (b *InsertStmtMpx) Columns(column ...string) *InsertStmtMpx {
	b.Column = column
	return b
}

// Comment adds a comment to prepended. All multi-line sql comment characters are stripped
func (b *InsertStmtMpx) Comment(comment string) *InsertStmtMpx {
	b.comments = b.comments.Append(comment)
	return b
}

// Ignore any insertion errors
func (b *InsertStmtMpx) Ignore() *InsertStmtMpx {
	b.Ignored = true
	return b
}

// Values adds a tuple to be inserted.
// The order of the tuple should match Columns.
func (b *InsertStmtMpx) Values(value ...interface{}) *InsertStmtMpx {
	b.Value = append(b.Value, value)
	return b
}

// Record adds a tuple for columns from a struct.
//
// If there is a field called "Id" or "ID" in the struct,
// it will be set to LastInsertId.
func (b *InsertStmtMpx) Record(structValue interface{}) *InsertStmtMpx {
	v := reflect.Indirect(reflect.ValueOf(structValue))

	if v.Kind() == reflect.Struct {
		found := make([]interface{}, len(b.Column)+1)
		// ID is recommended by golint here
		s := newTagStore()
		s.findValueByName(v, append(b.Column, "id"), found, false)

		value := found[:len(found)-1]
		for i, v := range value {
			if v != nil {
				value[i] = v.(reflect.Value).Interface()
			}
		}

		if v.CanSet() {
			switch idField := found[len(found)-1].(type) {
			case reflect.Value:
				if idField.Kind() == reflect.Int64 {
					id := idField.Addr().Interface().(*int64)
					b.PrimaryRecordID = id
					b.SecondaryRecordID = id
				}
			}
		}
		b.Values(value...)
	}
	return b
}

// Returning specifies the returning columns for postgres/mssql.
func (b *InsertStmtMpx) Returning(column ...string) *InsertStmtMpx {
	b.ReturnColumn = column
	return b
}

// Pair adds (column, value) to be inserted.
// It is an error to mix Pair with Values and Record.
func (b *InsertStmtMpx) Pair(column string, value interface{}) *InsertStmtMpx {
	b.Column = append(b.Column, column)
	switch len(b.Value) {
	case 0:
		b.Values(value)
	case 1:
		b.Value[0] = append(b.Value[0], value)
	default:
		panic("pair only allows one record to insert")
	}
	return b
}

func (b *InsertStmtMpx) Exec() (sql.Result, AsyncResultChan, error) {
	return b.ExecContext(context.Background())
}

func (b *InsertStmtMpx) ExecContext(ctx context.Context) (sql.Result, AsyncResultChan, error) {
	primaryRes, _, secondaryAsyncResultChan, err := b.ExecContextDebug(ctx)
	return primaryRes, secondaryAsyncResultChan, err
}

func (b *InsertStmtMpx) ExecContextDebug(ctx context.Context) (sql.Result, string, AsyncResultChan, error) {
	primaryResult, primaryQueryStr, secondaryAsyncResultChan, err := execMpx(ctx, b.RunnerMpx, b.PrimaryEventReceiver, b.SecondaryEventReceiver, b, b.PrimaryDialect, b.SecondaryDialect)
	if err != nil {
		return nil, primaryQueryStr, nil, err
	}

	if b.PrimaryRecordID != nil {
		if id, err := primaryResult.LastInsertId(); err == nil {
			*b.PrimaryRecordID = id
		}
		b.PrimaryRecordID = nil
	}

	secondaryErrChan := make(chan error)
	newSecondaryAsyncResultChan := make(AsyncResultChan)

	eg, egCtx := errgroup.WithContext(ctx)

	go func() {
		defer close(secondaryErrChan)
		secondaryErrChan <- eg.Wait()
	}()

	eg.Go(func() error {
		defer close(newSecondaryAsyncResultChan)

		for {
			select {
			case <-egCtx.Done():
				return egCtx.Err()
			case asyncResult, ok := <-secondaryAsyncResultChan:
				if !ok {
					return errors.New("failed to read result from asyncResultChan")
				}

				for {
					select {
					case <-egCtx.Done():
						return egCtx.Err()
					case rerr, ok := <-asyncResult.ErrChan:
						if !ok {
							return errors.New("failed to read error from errChan")
						}
						if rerr != nil {
							return rerr
						}

					default:
						if b.SecondaryRecordID != nil {
							if id, rerr := asyncResult.Result.LastInsertId(); rerr == nil {
								*b.SecondaryRecordID = id
							}
							b.SecondaryRecordID = nil
						}

						asyncResult.ErrChan = secondaryErrChan
						newSecondaryAsyncResultChan <- asyncResult
					}
				}
			}
		}
	})

	return primaryResult, primaryQueryStr, newSecondaryAsyncResultChan, nil
}

func (b *InsertStmtMpx) LoadContext(ctx context.Context, primaryValue, secondaryValue interface{}) (AsyncCountChan, error) {
	_, _, secondaryAsyncCountChan, err := queryMpx(ctx, b.RunnerMpx, b.PrimaryEventReceiver, b.SecondaryEventReceiver, b, b.PrimaryDialect, b.SecondaryDialect, primaryValue, secondaryValue)
	return secondaryAsyncCountChan, err
}

func (b *InsertStmtMpx) LoadContextDebug(ctx context.Context, primaryValue, secondaryValue interface{}) (string, AsyncCountChan, error) {
	_, primaryQueryStr, secondaryAsyncCountChan, err := queryMpx(ctx, b.RunnerMpx, b.PrimaryEventReceiver, b.SecondaryEventReceiver, b, b.PrimaryDialect, b.SecondaryDialect, primaryValue, secondaryValue)
	return primaryQueryStr, secondaryAsyncCountChan, err
}

func (b *InsertStmtMpx) Load(primaryValue, secondaryValue interface{}) (AsyncCountChan, error) {
	return b.LoadContext(context.Background(), primaryValue, secondaryValue)
}
