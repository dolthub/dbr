package dbr

import (
	"context"
	"database/sql"
	"strconv"
)

// UpdateStmtMpx builds `UPDATE ...`.
type UpdateStmtMpx struct {
	RunnerMpx

	PrimaryEventReceiver   EventReceiver
	SecondaryEventReceiver EventReceiver
	PrimaryDialect         Dialect
	SecondaryDialect       Dialect

	raw

	Table        string
	Value        map[string]interface{}
	WhereCond    []Builder
	ReturnColumn []string
	LimitCount   int64
	comments     Comments
	indexHints   []Builder
}

type UpdateBuilderMpx = UpdateStmtMpx

func (b *UpdateStmtMpx) Build(d Dialect, buf Buffer) error {
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

// UpdateMpx creates an UpdateStmt.
func UpdateMpx(table string) *UpdateStmtMpx {
	return &UpdateStmtMpx{
		Table:      table,
		Value:      make(map[string]interface{}),
		LimitCount: -1,
	}
}

// Update creates an UpdateStmtMpx.
func (smpx *SessionMpx) Update(table string) *UpdateStmtMpx {
	b := UpdateMpx(table)
	b.RunnerMpx = smpx

	b.PrimaryEventReceiver = smpx.PrimaryEventReceiver
	b.PrimaryDialect = smpx.PrimaryConn.Dialect

	b.SecondaryEventReceiver = smpx.SecondaryEventReceiver
	b.SecondaryDialect = smpx.SecondaryConn.Dialect
	return b
}

// Update creates an UpdateStmt.
func (txMpx *TxMpx) Update(table string) *UpdateStmtMpx {
	b := UpdateMpx(table)
	b.RunnerMpx = txMpx

	b.PrimaryEventReceiver = txMpx.PrimaryTx.EventReceiver
	b.PrimaryDialect = txMpx.PrimaryTx.Dialect

	b.SecondaryEventReceiver = txMpx.SecondaryTx.EventReceiver
	b.SecondaryDialect = txMpx.SecondaryTx.Dialect
	return b
}

// UpdateBySqlMpx creates an UpdateStmtMpx with raw query.
func UpdateBySqlMpx(query string, value ...interface{}) *UpdateStmtMpx {
	return &UpdateStmtMpx{
		raw: raw{
			Query: query,
			Value: value,
		},
		Value:      make(map[string]interface{}),
		LimitCount: -1,
	}
}

// UpdateBySqlMpx creates an UpdateStmt with raw query.
func (smpx *SessionMpx) UpdateBySql(query string, value ...interface{}) *UpdateStmtMpx {
	b := UpdateBySqlMpx(query, value...)
	b.RunnerMpx = smpx

	b.PrimaryEventReceiver = smpx.PrimaryEventReceiver
	b.PrimaryDialect = smpx.PrimaryConn.Dialect

	b.SecondaryEventReceiver = smpx.SecondaryEventReceiver
	b.SecondaryDialect = smpx.SecondaryConn.Dialect
	return b
}

// UpdateBySql creates an UpdateStmt with raw query.
func (txMpx *TxMpx) UpdateBySql(query string, value ...interface{}) *UpdateStmtMpx {
	b := UpdateBySqlMpx(query, value...)
	b.RunnerMpx = txMpx

	b.PrimaryEventReceiver = txMpx.PrimaryTx.EventReceiver
	b.PrimaryDialect = txMpx.PrimaryTx.Dialect

	b.SecondaryEventReceiver = txMpx.SecondaryTx.EventReceiver
	b.SecondaryDialect = txMpx.SecondaryTx.Dialect
	return b
}

// Where adds a where condition.
// query can be Builder or string. value is used only if query type is string.
func (b *UpdateStmtMpx) Where(query interface{}, value ...interface{}) *UpdateStmtMpx {
	switch query := query.(type) {
	case string:
		b.WhereCond = append(b.WhereCond, Expr(query, value...))
	case Builder:
		b.WhereCond = append(b.WhereCond, query)
	}
	return b
}

// Returning specifies the returning columns for postgres.
func (b *UpdateStmtMpx) Returning(column ...string) *UpdateStmtMpx {
	b.ReturnColumn = column
	return b
}

// Set updates column with value.
func (b *UpdateStmtMpx) Set(column string, value interface{}) *UpdateStmtMpx {
	b.Value[column] = value
	return b
}

// SetMap specifies a map of (column, value) to update in bulk.
func (b *UpdateStmtMpx) SetMap(m map[string]interface{}) *UpdateStmtMpx {
	for col, val := range m {
		b.Set(col, val)
	}
	return b
}

// IncrBy increases column by value
func (b *UpdateStmtMpx) IncrBy(column string, value interface{}) *UpdateStmtMpx {
	b.Value[column] = Expr("? + ?", I(column), value)
	return b
}

// DecrBy decreases column by value
func (b *UpdateStmtMpx) DecrBy(column string, value interface{}) *UpdateStmtMpx {
	b.Value[column] = Expr("? - ?", I(column), value)
	return b
}

func (b *UpdateStmtMpx) Limit(n uint64) *UpdateStmtMpx {
	b.LimitCount = int64(n)
	return b
}

func (b *UpdateStmtMpx) Comment(comment string) *UpdateStmtMpx {
	b.comments = b.comments.Append(comment)
	return b
}

func (b *UpdateStmtMpx) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

func (b *UpdateStmtMpx) ExecContext(ctx context.Context) (sql.Result, error) {
	primaryRes, _, err := execMpx(ctx, b.RunnerMpx, b.PrimaryEventReceiver, b.SecondaryEventReceiver, b, b.PrimaryDialect, b.SecondaryDialect)
	return primaryRes, err
}

func (b *UpdateStmtMpx) ExecContextDebug(ctx context.Context) (sql.Result, string, error) {
	return execMpx(ctx, b.RunnerMpx, b.PrimaryEventReceiver, b.SecondaryEventReceiver, b, b.PrimaryDialect, b.SecondaryDialect)
}

func (b *UpdateStmtMpx) LoadContext(ctx context.Context, primaryValue interface{}) error {
	_, _, err := queryMpx(ctx, b.RunnerMpx, b.PrimaryEventReceiver, b.SecondaryEventReceiver, b, b.PrimaryDialect, b.SecondaryDialect, primaryValue)
	return err
}

func (b *UpdateStmtMpx) LoadContextDebug(ctx context.Context, primaryValue interface{}) (string, error) {
	_, primaryQueryStr, err := queryMpx(ctx, b.RunnerMpx, b.PrimaryEventReceiver, b.SecondaryEventReceiver, b, b.PrimaryDialect, b.SecondaryDialect, primaryValue)
	return primaryQueryStr, err
}

func (b *UpdateStmtMpx) Load(primaryValue interface{}) error {
	return b.LoadContext(context.Background(), primaryValue)
}

// IndexHint adds a index hint.
// hint can be Builder or string.
func (b *UpdateStmtMpx) IndexHint(hints ...interface{}) *UpdateStmtMpx {
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
