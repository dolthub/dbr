package dbr

import (
	"testing"

	"github.com/gocraft/dbr/v2/dialect"
	"github.com/stretchr/testify/require"
)

func TestUpdateStmtMpx(t *testing.T) {
	buf := NewBuffer()
	builder := UpdateMpx("table").Set("a", 1).Where(Eq("b", 2)).Comment("UPDATE TEST").
		IndexHint(ForceIndex("idx_a_b"))
	err := builder.Build(dialect.MySQL, buf)
	require.NoError(t, err)

	require.Equal(t, "/* UPDATE TEST */\nUPDATE `table` FORCE INDEX(`idx_a_b`) SET `a` = ? WHERE (`b` = ?)", buf.String())
	require.Equal(t, []interface{}{1, 2}, buf.Value())
}

func BenchmarkUpdateMpxValuesSQL(b *testing.B) {
	buf := NewBuffer()
	for i := 0; i < b.N; i++ {
		UpdateMpx("table").Set("a", 1).Build(dialect.MySQL, buf)
	}
}

func BenchmarkUpdateMpxMapSQL(b *testing.B) {
	buf := NewBuffer()
	for i := 0; i < b.N; i++ {
		UpdateMpx("table").SetMap(map[string]interface{}{"a": 1, "b": 2}).Build(dialect.MySQL, buf)
	}
}

func TestUpdateMpxIncrBy(t *testing.T) {
	buf := NewBuffer()
	builder := UpdateMpx("table").IncrBy("a", 1).Where(Eq("b", 2))
	err := builder.Build(dialect.MySQL, buf)
	require.NoError(t, err)

	sqlstr, err := InterpolateForDialect(buf.String(), buf.Value(), dialect.MySQL)
	require.NoError(t, err)

	require.Equal(t, "UPDATE `table` SET `a` = `a` + 1 WHERE (`b` = 2)", sqlstr)
}
