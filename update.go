package dbr

import (
	"context"
	"database/sql"
	"strconv"
)

// UpdateStmt builds `UPDATE ...`.
type UpdateStmt struct {
	runner
	EventReceiver
	Dialect

	raw

	Table        string
	Value        map[string]interface{}
	WhereCond    []Builder
	ReturnColumn []string
	LimitCount   int64
	comments     Comments
	indexHints   []Builder

	exec  func(ctx context.Context, builder Builder) (sql.Result, string, error)
	query func(ctx context.Context, builder Builder, dest interface{}) (string, error)
}

type UpdateBuilder = UpdateStmt

func (b *UpdateStmt) Build(d Dialect, buf Buffer) error {
	if b.raw.Query != "" {
		return b.raw.Build(d, buf)
	}

	if b.Table == "" {
		return ErrTableNotSpecified
	}

	if len(b.Value) == 0 {
		return ErrColumnNotSpecified
	}

	err := b.comments.Build(d, buf)
	if err != nil {
		return err
	}

	buf.WriteString("UPDATE ")
	buf.WriteString(d.QuoteIdent(b.Table))
	for _, hint := range b.indexHints {
		buf.WriteString(" ")
		if err := hint.Build(d, buf); err != nil {
			return err
		}
	}
	buf.WriteString(" SET ")

	i := 0
	for col, v := range b.Value {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(d.QuoteIdent(col))
		buf.WriteString(" = ")
		buf.WriteString(placeholder)

		buf.WriteValue(v)

		i++
	}

	if len(b.WhereCond) > 0 {
		buf.WriteString(" WHERE ")
		err := And(b.WhereCond...).Build(d, buf)
		if err != nil {
			return err
		}
	}

	if len(b.ReturnColumn) > 0 {
		buf.WriteString(" RETURNING ")
		for i, col := range b.ReturnColumn {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(d.QuoteIdent(col))
		}
	}

	if b.LimitCount >= 0 {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strconv.FormatInt(b.LimitCount, 10))
	}

	return nil
}

// Update creates an UpdateStmt.
func Update(table string) *UpdateStmt {
	return &UpdateStmt{
		Table:      table,
		Value:      make(map[string]interface{}),
		LimitCount: -1,
	}
}

// Update creates an UpdateStmt.
func (sess *Session) Update(table string) *UpdateStmt {
	b := Update(table)
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

// Update creates an UpdateStmt.
func (smpx *SessionMpx) Update(table string) *UpdateStmt {
	b := Update(table)
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

// Update creates an UpdateStmt.
func (tx *Tx) Update(table string) *UpdateStmt {
	b := Update(table)
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

// Update creates an UpdateStmt.
func (txMpx *TxMpx) Update(table string) *UpdateStmt {
	b := Update(table)
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

// UpdateBySql creates an UpdateStmt with raw query.
func UpdateBySql(query string, value ...interface{}) *UpdateStmt {
	return &UpdateStmt{
		raw: raw{
			Query: query,
			Value: value,
		},
		Value:      make(map[string]interface{}),
		LimitCount: -1,
	}
}

// UpdateBySql creates an UpdateStmt with raw query.
func (sess *Session) UpdateBySql(queryStr string, value ...interface{}) *UpdateStmt {
	b := UpdateBySql(queryStr, value...)
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

// UpdateBySql creates an UpdateStmt with raw query.
func (smpx *SessionMpx) UpdateBySql(query string, value ...interface{}) *UpdateStmt {
	b := UpdateBySql(query, value...)
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

// UpdateBySql creates an UpdateStmt with raw query.
func (tx *Tx) UpdateBySql(queryStr string, value ...interface{}) *UpdateStmt {
	b := UpdateBySql(queryStr, value...)
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

// UpdateBySql creates an UpdateStmt with raw query.
func (txMpx *TxMpx) UpdateBySql(query string, value ...interface{}) *UpdateStmt {
	b := UpdateBySql(query, value...)
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

// Where adds a where condition.
// query can be Builder or string. value is used only if query type is string.
func (b *UpdateStmt) Where(query interface{}, value ...interface{}) *UpdateStmt {
	switch query := query.(type) {
	case string:
		b.WhereCond = append(b.WhereCond, Expr(query, value...))
	case Builder:
		b.WhereCond = append(b.WhereCond, query)
	}
	return b
}

// Returning specifies the returning columns for postgres.
func (b *UpdateStmt) Returning(column ...string) *UpdateStmt {
	b.ReturnColumn = column
	return b
}

// Set updates column with value.
func (b *UpdateStmt) Set(column string, value interface{}) *UpdateStmt {
	b.Value[column] = value
	return b
}

// SetMap specifies a map of (column, value) to update in bulk.
func (b *UpdateStmt) SetMap(m map[string]interface{}) *UpdateStmt {
	for col, val := range m {
		b.Set(col, val)
	}
	return b
}

// IncrBy increases column by value
func (b *UpdateStmt) IncrBy(column string, value interface{}) *UpdateStmt {
	b.Value[column] = Expr("? + ?", I(column), value)
	return b
}

// DecrBy decreases column by value
func (b *UpdateStmt) DecrBy(column string, value interface{}) *UpdateStmt {
	b.Value[column] = Expr("? - ?", I(column), value)
	return b
}

func (b *UpdateStmt) Limit(n uint64) *UpdateStmt {
	b.LimitCount = int64(n)
	return b
}

func (b *UpdateStmt) Comment(comment string) *UpdateStmt {
	b.comments = b.comments.Append(comment)
	return b
}

func (b *UpdateStmt) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

func (b *UpdateStmt) ExecContext(ctx context.Context) (sql.Result, error) {
	res, _, err := b.exec(ctx, b)
	return res, err
}

func (b *UpdateStmt) ExecContextDebug(ctx context.Context) (sql.Result, string, error) {
	return b.exec(ctx, b)
}

func (b *UpdateStmt) LoadContext(ctx context.Context, value interface{}) error {
	_, err := b.query(ctx, b, value)
	return err
}

func (b *UpdateStmt) LoadContextDebug(ctx context.Context, value interface{}) (string, error) {
	return b.query(ctx, b, value)
}

func (b *UpdateStmt) Load(value interface{}) error {
	return b.LoadContext(context.Background(), value)
}

// IndexHint adds a index hint.
// hint can be Builder or string.
func (b *UpdateStmt) IndexHint(hints ...interface{}) *UpdateStmt {
	for _, hint := range hints {
		switch hint := hint.(type) {
		case string:
			b.indexHints = append(b.indexHints, Expr(hint))
		case Builder:
			b.indexHints = append(b.indexHints, hint)
		}
	}
	return b
}
