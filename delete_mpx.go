package dbr

import (
	"context"
	"database/sql"
	"strconv"
)

// DeleteStmtMpx builds `DELETE ...`.
type DeleteStmtMpx struct {
	RunnerMpx

	PrimaryEventReceiver   EventReceiver
	SecondaryEventReceiver EventReceiver

	PrimaryDialect   Dialect
	SecondaryDialect Dialect

	raw

	Table      string
	WhereCond  []Builder
	LimitCount int64

	comments Comments
}

type DeleteBuilderMpx = DeleteStmtMpx

func (b *DeleteStmtMpx) Build(d Dialect, buf Buffer) error {
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

// DeleteFromMpx creates a DeleteStmtMpx.
func DeleteFromMpx(table string) *DeleteStmtMpx {
	return &DeleteStmtMpx{
		Table:      table,
		LimitCount: -1,
	}
}

// DeleteFromMpx creates a DeleteStmtMpx.
func (sessMpx *SessionMpx) DeleteFrom(table string) *DeleteStmtMpx {
	b := DeleteFromMpx(table)
	b.RunnerMpx = sessMpx

	b.PrimaryEventReceiver = sessMpx.PrimaryEventReceiver
	b.SecondaryEventReceiver = sessMpx.SecondaryEventReceiver

	b.PrimaryDialect = sessMpx.PrimaryConn.Dialect
	b.SecondaryDialect = sessMpx.SecondaryConn.Dialect

	return b
}

// DeleteFromMpx creates a DeleteStmtMpx.
func (txMpx *TxMpx) DeleteFrom(table string) *DeleteStmtMpx {
	b := DeleteFromMpx(table)

	b.RunnerMpx = txMpx

	b.PrimaryEventReceiver = txMpx.PrimaryTx.EventReceiver
	b.SecondaryEventReceiver = txMpx.SecondaryTx.EventReceiver

	b.PrimaryDialect = txMpx.PrimaryTx.Dialect
	b.SecondaryDialect = txMpx.SecondaryTx.Dialect

	return b
}

// DeleteBySqlMpx creates a DeleteStmtMpx from raw query.
func DeleteBySqlMpx(query string, value ...interface{}) *DeleteStmtMpx {
	return &DeleteStmtMpx{
		raw: raw{
			Query: query,
			Value: value,
		},
		LimitCount: -1,
	}
}

// DeleteBySqlMpx creates a DeleteStmtMpx from raw query.
func (sessMpx *SessionMpx) DeleteBySql(query string, value ...interface{}) *DeleteStmtMpx {
	b := DeleteBySqlMpx(query, value...)

	b.RunnerMpx = sessMpx

	b.PrimaryEventReceiver = sessMpx.PrimaryEventReceiver
	b.SecondaryEventReceiver = sessMpx.SecondaryEventReceiver

	b.PrimaryDialect = sessMpx.PrimaryConn.Dialect
	b.SecondaryDialect = sessMpx.SecondaryConn.Dialect

	return b
}

// DeleteBySqlMpx creates a DeleteStmtMpx from raw query.
func (txMpx *TxMpx) DeleteBySql(query string, value ...interface{}) *DeleteStmtMpx {
	b := DeleteBySqlMpx(query, value...)

	b.RunnerMpx = txMpx

	b.PrimaryEventReceiver = txMpx.PrimaryTx.EventReceiver
	b.SecondaryEventReceiver = txMpx.SecondaryTx.EventReceiver

	b.PrimaryDialect = txMpx.PrimaryTx.Dialect
	b.SecondaryDialect = txMpx.SecondaryTx.Dialect

	return b
}

// Where adds a where condition.
// query can be Builder or string. value is used only if query type is string.
func (b *DeleteStmtMpx) Where(query interface{}, value ...interface{}) *DeleteStmtMpx {
	switch query := query.(type) {
	case string:
		b.WhereCond = append(b.WhereCond, Expr(query, value...))
	case Builder:
		b.WhereCond = append(b.WhereCond, query)
	}
	return b
}

func (b *DeleteStmtMpx) Limit(n uint64) *DeleteStmtMpx {
	b.LimitCount = int64(n)
	return b
}

func (b *DeleteStmtMpx) Comment(comment string) *DeleteStmtMpx {
	b.comments = b.comments.Append(comment)
	return b
}

func (b *DeleteStmtMpx) Exec() (sql.Result, AsyncResultChan, error) {
	return b.ExecContext(context.Background())
}

func (b *DeleteStmtMpx) ExecContext(ctx context.Context) (sql.Result, AsyncResultChan, error) {
	primaryRes, _, secondaryAsyncResultChan, err := execMpx(ctx, b.RunnerMpx, b.PrimaryEventReceiver, b.SecondaryEventReceiver, b, b.PrimaryDialect, b.SecondaryDialect)
	return primaryRes, secondaryAsyncResultChan, err
}

func (b *DeleteStmtMpx) ExecContextDebug(ctx context.Context) (sql.Result, string, AsyncResultChan, error) {
	return execMpx(ctx, b.RunnerMpx, b.PrimaryEventReceiver, b.SecondaryEventReceiver, b, b.PrimaryDialect, b.SecondaryDialect)
}
