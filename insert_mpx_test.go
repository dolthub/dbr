package dbr

import (
	"testing"

	"github.com/gocraft/dbr/v2/dialect"
	"github.com/stretchr/testify/require"
)

func TestInsertStmtMpx(t *testing.T) {
	buf := NewBuffer()
	builder := InsertIntoMpx("table").Ignore().Columns("a", "b").Values(1, "one").Record(&insertTest{
		A: 2,
		C: "two",
	}).Comment("INSERT TEST")
	err := builder.Build(dialect.MySQL, buf)
	require.NoError(t, err)
	require.Equal(t, "/* INSERT TEST */\nINSERT IGNORE INTO `table` (`a`,`b`) VALUES (?,?), (?,?)", buf.String())
	require.Equal(t, []interface{}{1, "one", 2, "two"}, buf.Value())
}

func BenchmarkInsertValuesSQLMpx(b *testing.B) {
	buf := NewBuffer()
	for i := 0; i < b.N; i++ {
		InsertIntoMpx("table").Columns("a", "b").Values(1, "one").Build(dialect.MySQL, buf)
	}
}

func BenchmarkInsertRecordSQLMpx(b *testing.B) {
	buf := NewBuffer()
	for i := 0; i < b.N; i++ {
		InsertIntoMpx("table").Columns("a", "b").Record(&insertTest{
			A: 2,
			C: "two",
		}).Build(dialect.MySQL, buf)
	}
}
