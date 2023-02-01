package dbr

import (
	"context"
	"database/sql"
	"strconv"
)

// DeleteStmt builds `DELETE ...`.
type DeleteStmt struct {
	runner
	EventReceiver
	Dialect

	raw

	Table      string
	WhereCond  []Builder
	LimitCount int64

	comments Comments

	exec func(ctx context.Context, builder Builder) (sql.Result, string, error)
}

type DeleteBuilder = DeleteStmt

func (b *DeleteStmt) Build(d Dialect, buf Buffer) error {
	if b.raw.Query != "" {
		return b.raw.Build(d, buf)
	}

	if b.Table == "" {
		return ErrTableNotSpecified
	}

	err := b.comments.Build(d, buf)
	if err != nil {
		return err
	}

	buf.WriteString("DELETE FROM ")
	buf.WriteString(d.QuoteIdent(b.Table))

	if len(b.WhereCond) > 0 {
		buf.WriteString(" WHERE ")
		err := And(b.WhereCond...).Build(d, buf)
		if err != nil {
			return err
		}
	}
	if b.LimitCount >= 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strconv.FormatInt(b.LimitCount, 10))
	}
	return nil
}

// DeleteFrom creates a DeleteStmt.
func DeleteFrom(table string) *DeleteStmt {
	return &DeleteStmt{
		Table:      table,
		LimitCount: -1,
	}
}

// DeleteFrom creates a DeleteStmt.
func (sess *Session) DeleteFrom(table string) *DeleteStmt {
	b := DeleteFrom(table)
	b.runner = sess
	b.EventReceiver = sess.EventReceiver
	b.Dialect = sess.Dialect
	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return exec(ctx, sess, sess.EventReceiver, b, sess.Dialect)
	}
	return b
}

// DeleteFrom creates a DeleteStmt.
func (smpx *SessionMpx) DeleteFrom(table string) *DeleteStmt {
	b := DeleteFrom(table)
	b.runner = smpx

	b.EventReceiver = smpx.PrimaryEventReceiver
	b.Dialect = smpx.PrimaryConn.Dialect

	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return execMpx(ctx, smpx, smpx.PrimaryEventReceiver, smpx.SecondaryEventReceiver, b, smpx.PrimaryConn.Dialect, smpx.SecondaryConn.Dialect)
	}
	return b
}

// DeleteFrom creates a DeleteStmt.
func (tx *Tx) DeleteFrom(table string) *DeleteStmt {
	b := DeleteFrom(table)
	b.runner = tx
	b.EventReceiver = tx.EventReceiver
	b.Dialect = tx.Dialect
	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return exec(ctx, tx, tx.EventReceiver, b, tx.Dialect)
	}
	return b
}

// DeleteFrom creates a DeleteStmt.
func (txMpx *TxMpx) DeleteFrom(table string) *DeleteStmt {
	b := DeleteFrom(table)
	b.runner = txMpx

	b.EventReceiver = txMpx.PrimaryTx.EventReceiver
	b.Dialect = txMpx.PrimaryTx.Dialect

	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return execMpx(ctx, txMpx, txMpx.PrimaryTx.EventReceiver, txMpx.SecondaryTx.EventReceiver, b, txMpx.PrimaryTx.Dialect, txMpx.SecondaryTx.Dialect)
	}
	return b
}

// DeleteBySql creates a DeleteStmt from raw query.
func DeleteBySql(query string, value ...interface{}) *DeleteStmt {
	return &DeleteStmt{
		raw: raw{
			Query: query,
			Value: value,
		},
		LimitCount: -1,
	}
}

// DeleteBySql creates a DeleteStmt from raw query.
func (sess *Session) DeleteBySql(queryStr string, value ...interface{}) *DeleteStmt {
	b := DeleteBySql(queryStr, value...)
	b.runner = sess
	b.EventReceiver = sess.EventReceiver
	b.Dialect = sess.Dialect
	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return exec(ctx, sess, sess.EventReceiver, b, sess.Dialect)
	}
	return b
}

// DeleteBySql creates a DeleteStmt from raw query.
func (smpx *SessionMpx) DeleteBySql(query string, value ...interface{}) *DeleteStmt {
	b := DeleteBySql(query, value...)
	b.runner = smpx

	b.EventReceiver = smpx.PrimaryEventReceiver
	b.Dialect = smpx.PrimaryConn.Dialect

	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return execMpx(ctx, smpx, smpx.PrimaryEventReceiver, smpx.SecondaryEventReceiver, b, smpx.PrimaryConn.Dialect, smpx.SecondaryConn.Dialect)
	}
	return b
}

// DeleteBySql creates a DeleteStmt from raw query.
func (tx *Tx) DeleteBySql(queryStr string, value ...interface{}) *DeleteStmt {
	b := DeleteBySql(queryStr, value...)
	b.runner = tx
	b.EventReceiver = tx.EventReceiver
	b.Dialect = tx.Dialect
	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return exec(ctx, tx, tx.EventReceiver, b, tx.Dialect)
	}
	return b
}

// DeleteBySql creates a DeleteStmt from raw query.
func (txMpx *TxMpx) DeleteBySql(query string, value ...interface{}) *DeleteStmt {
	b := DeleteBySql(query, value...)
	b.runner = txMpx

	b.EventReceiver = txMpx.PrimaryTx.EventReceiver
	b.Dialect = txMpx.PrimaryTx.Dialect

	b.exec = func(ctx context.Context, builder Builder) (sql.Result, string, error) {
		return execMpx(ctx, txMpx, txMpx.PrimaryTx.EventReceiver, txMpx.SecondaryTx.EventReceiver, b, txMpx.PrimaryTx.Dialect, txMpx.SecondaryTx.Dialect)
	}
	return b
}

// Where adds a where condition.
// query can be Builder or string. value is used only if query type is string.
func (b *DeleteStmt) Where(query interface{}, value ...interface{}) *DeleteStmt {
	switch query := query.(type) {
	case string:
		b.WhereCond = append(b.WhereCond, Expr(query, value...))
	case Builder:
		b.WhereCond = append(b.WhereCond, query)
	}
	return b
}

func (b *DeleteStmt) Limit(n uint64) *DeleteStmt {
	b.LimitCount = int64(n)
	return b
}

func (b *DeleteStmt) Comment(comment string) *DeleteStmt {
	b.comments = b.comments.Append(comment)
	return b
}

func (b *DeleteStmt) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

func (b *DeleteStmt) ExecContext(ctx context.Context) (sql.Result, error) {
	res, _, err := b.exec(ctx, b)
	return res, err
}

func (b *DeleteStmt) ExecContextDebug(ctx context.Context) (sql.Result, string, error) {
	return b.exec(ctx, b)
}
