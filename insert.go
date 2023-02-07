package dbr

import (
	"context"
	"database/sql"
	"reflect"
	"strings"

	"github.com/gocraft/dbr/v2/dialect"
)

// InsertStmt builds `INSERT INTO ...`.
type InsertStmt struct {
	runner
	EventReceiver
	Dialect

	raw

	Table        string
	Column       []string
	Value        [][]interface{}
	Ignored      bool
	ReturnColumn []string
	RecordID     *int64
	comments     Comments

	exec  func(ctx context.Context, builder Builder) (sql.Result, string, error)
	query func(ctx context.Context, builder Builder, dest interface{}) (string, error)
}

type InsertBuilder = InsertStmt

func (b *InsertStmt) Build(d Dialect, buf Buffer) error {
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

	if d != dialect.MSSQL && d != dialect.MySQL && len(b.ReturnColumn) > 0 {
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

// InsertInto creates an InsertStmt.
func InsertInto(table string) *InsertStmt {
	return &InsertStmt{
		Table: table,
	}
}

// InsertInto creates an InsertStmt.
func (sess *Session) InsertInto(table string) *InsertStmt {
	b := InsertInto(table)
	b.runner = sess
	b.EventReceiver = sess.EventReceiver
	b.Dialect = sess.Dialect
	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return exec(ctx, sess, sess.EventReceiver, b, sess.Dialect)
	}
	b.query = func(ctx context.Context, builder Builder, dest interface{}) (string, error) {
		_, qStr, err := query(ctx, sess, sess.EventReceiver, b, sess.Dialect, dest)
		return qStr, err
	}
	return b
}

// InsertInto creates an InsertStmt.
func (smpx *SessionMpx) InsertInto(table string) *InsertStmt {
	b := InsertInto(table)
	b.runner = smpx

	b.EventReceiver = smpx.PrimaryEventReceiver
	b.Dialect = smpx.PrimaryConn.Dialect

	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return execMpx(ctx, smpx, smpx.PrimaryEventReceiver, smpx.SecondaryEventReceiver, b, smpx.PrimaryConn.Dialect, smpx.SecondaryConn.Dialect)
	}
	b.query = func(ctx context.Context, builder Builder, dest interface{}) (string, error) {
		_, qStr, err := queryMpx(ctx, smpx, smpx.PrimaryEventReceiver, smpx.SecondaryEventReceiver, b, smpx.PrimaryConn.Dialect, smpx.SecondaryConn.Dialect, dest)
		return qStr, err
	}
	return b
}

// InsertInto creates an InsertStmt.
func (tx *Tx) InsertInto(table string) *InsertStmt {
	b := InsertInto(table)
	b.runner = tx
	b.EventReceiver = tx.EventReceiver
	b.Dialect = tx.Dialect
	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return exec(ctx, tx, tx.EventReceiver, b, tx.Dialect)
	}
	b.query = func(ctx context.Context, builder Builder, dest interface{}) (string, error) {
		_, qStr, err := query(ctx, tx, tx.EventReceiver, b, tx.Dialect, dest)
		return qStr, err
	}
	return b
}

// InsertInto creates an InsertStmt.
func (txMpx *TxMpx) InsertInto(table string) *InsertStmt {
	b := InsertInto(table)
	b.runner = txMpx

	b.EventReceiver = txMpx.PrimaryTx.EventReceiver
	b.Dialect = txMpx.PrimaryTx.Dialect

	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return execMpx(ctx, txMpx, txMpx.PrimaryTx.EventReceiver, txMpx.SecondaryTx.EventReceiver, b, txMpx.PrimaryTx.Dialect, txMpx.SecondaryTx.Dialect)
	}
	b.query = func(ctx context.Context, builder Builder, dest interface{}) (string, error) {
		_, qStr, err := queryMpx(ctx, txMpx, txMpx.PrimaryTx.EventReceiver, txMpx.SecondaryTx.EventReceiver, b, txMpx.PrimaryTx.Dialect, txMpx.SecondaryTx.Dialect, dest)
		return qStr, err
	}
	return b
}

// InsertBySql creates an InsertStmt from raw query.
func InsertBySql(query string, value ...interface{}) *InsertStmt {
	return &InsertStmt{
		raw: raw{
			Query: query,
			Value: value,
		},
	}
}

// InsertBySql creates an InsertStmt from raw query.
func (sess *Session) InsertBySql(queryStr string, value ...interface{}) *InsertStmt {
	b := InsertBySql(queryStr, value...)
	b.runner = sess
	b.EventReceiver = sess.EventReceiver
	b.Dialect = sess.Dialect
	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return exec(ctx, sess, sess.EventReceiver, b, sess.Dialect)
	}
	b.query = func(ctx context.Context, builder Builder, dest interface{}) (string, error) {
		_, qStr, err := query(ctx, sess, sess.EventReceiver, b, sess.Dialect, dest)
		return qStr, err
	}
	return b
}

// InsertBySql creates an InsertStmt from raw query.
func (smpx *SessionMpx) InsertBySql(query string, value ...interface{}) *InsertStmt {
	b := InsertBySql(query, value...)
	b.runner = smpx

	b.EventReceiver = smpx.PrimaryEventReceiver
	b.Dialect = smpx.PrimaryConn.Dialect

	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return execMpx(ctx, smpx, smpx.PrimaryEventReceiver, smpx.SecondaryEventReceiver, b, smpx.PrimaryConn.Dialect, smpx.SecondaryConn.Dialect)
	}
	b.query = func(ctx context.Context, builder Builder, dest interface{}) (string, error) {
		_, qStr, err := queryMpx(ctx, smpx, smpx.PrimaryEventReceiver, smpx.SecondaryEventReceiver, b, smpx.PrimaryConn.Dialect, smpx.SecondaryConn.Dialect, dest)
		return qStr, err
	}
	return b
}

// InsertBySql creates an InsertStmt from raw query.
func (tx *Tx) InsertBySql(queryStr string, value ...interface{}) *InsertStmt {
	b := InsertBySql(queryStr, value...)
	b.runner = tx
	b.EventReceiver = tx.EventReceiver
	b.Dialect = tx.Dialect
	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return exec(ctx, tx, tx.EventReceiver, b, tx.Dialect)
	}
	b.query = func(ctx context.Context, builder Builder, dest interface{}) (string, error) {
		_, qStr, err := query(ctx, tx, tx.EventReceiver, b, tx.Dialect, dest)
		return qStr, err
	}
	return b
}

// InsertBySql creates an InsertStmt from raw query.
func (txMpx *TxMpx) InsertBySql(query string, value ...interface{}) *InsertStmt {
	b := InsertBySql(query, value...)
	b.runner = txMpx

	b.EventReceiver = txMpx.PrimaryTx.EventReceiver
	b.Dialect = txMpx.PrimaryTx.Dialect

	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return execMpx(ctx, txMpx, txMpx.PrimaryTx.EventReceiver, txMpx.SecondaryTx.EventReceiver, b, txMpx.PrimaryTx.Dialect, txMpx.SecondaryTx.Dialect)
	}
	b.query = func(ctx context.Context, builder Builder, dest interface{}) (string, error) {
		_, qStr, err := queryMpx(ctx, txMpx, txMpx.PrimaryTx.EventReceiver, txMpx.SecondaryTx.EventReceiver, b, txMpx.PrimaryTx.Dialect, txMpx.SecondaryTx.Dialect, dest)
		return qStr, err
	}
	return b
}

func (b *InsertStmt) Columns(column ...string) *InsertStmt {
	b.Column = column
	return b
}

// Comment adds a comment to prepended. All multi-line sql comment characters are stripped
func (b *InsertStmt) Comment(comment string) *InsertStmt {
	b.comments = b.comments.Append(comment)
	return b
}

// Ignore any insertion errors
func (b *InsertStmt) Ignore() *InsertStmt {
	b.Ignored = true
	return b
}

// Values adds a tuple to be inserted.
// The order of the tuple should match Columns.
func (b *InsertStmt) Values(value ...interface{}) *InsertStmt {
	b.Value = append(b.Value, value)
	return b
}

// Record adds a tuple for columns from a struct.
//
// If there is a field called "Id" or "ID" in the struct,
// it will be set to LastInsertId.
func (b *InsertStmt) Record(structValue interface{}) *InsertStmt {
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
					b.RecordID = idField.Addr().Interface().(*int64)
				}
			}
		}
		b.Values(value...)
	}
	return b
}

// Returning specifies the returning columns for postgres/mssql.
func (b *InsertStmt) Returning(column ...string) *InsertStmt {
	b.ReturnColumn = column
	return b
}

// Pair adds (column, value) to be inserted.
// It is an error to mix Pair with Values and Record.
func (b *InsertStmt) Pair(column string, value interface{}) *InsertStmt {
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

func (b *InsertStmt) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

func (b *InsertStmt) ExecContext(ctx context.Context) (sql.Result, error) {
	res, _, err := b.ExecContextDebug(ctx)
	return res, err
}

func (b *InsertStmt) ExecContextDebug(ctx context.Context) (sql.Result, string, error) {
	result, queryStr, err := b.exec(ctx, b)
	if err != nil {
		return nil, queryStr, err
	}

	if b.RecordID != nil {
		if id, err := result.LastInsertId(); err == nil {
			*b.RecordID = id
		}
		b.RecordID = nil
	}

	return result, queryStr, nil
}

func (b *InsertStmt) LoadContext(ctx context.Context, value interface{}) error {
	_, err := b.query(ctx, b, value)
	return err
}

func (b *InsertStmt) LoadContextDebug(ctx context.Context, value interface{}) (string, error) {
	return b.query(ctx, b, value)
}

func (b *InsertStmt) Load(value interface{}) error {
	return b.LoadContext(context.Background(), value)
}
