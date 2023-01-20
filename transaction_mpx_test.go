package dbr

import (
	"testing"

	"github.com/gocraft/dbr/v2/dialect"
	"github.com/stretchr/testify/require"
)

func TestTransactionCommitMpx(t *testing.T) {
	for _, sessMpx := range testSessionMpx {
		resetMpx(t, sessMpx)

		txMpx, err := sessMpx.Begin()
		require.NoError(t, err)
		defer txMpx.RollbackUnlessCommitted()

		elem_count := 1
		if sessMpx.PrimaryConn.Dialect == dialect.MSSQL || sessMpx.SecondaryConn.Dialect == dialect.MSSQL {
			txMpx.UpdateBySql("SET IDENTITY_INSERT dbr_people ON;").Exec()
			elem_count += 1
		}

		id := 1
		primaryResult, secondaryResult, err := txMpx.InsertInto("dbr_people").Columns("id", "name", "email").Values(id, "Barack", "obama@whitehouse.gov").Comment("INSERT TEST").Exec()
		require.NoError(t, err)

		require.Len(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started, elem_count)
		require.Len(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started, elem_count)

		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].eventName, "dbr.primary.exec")
		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "/* INSERT TEST */\n")
		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "INSERT")
		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "dbr_people")
		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "name")
		require.Equal(t, elem_count, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).finished)
		require.Equal(t, 0, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).errored)

		require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[elem_count-1].eventName, "dbr.secondary.exec")
		require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "/* INSERT TEST */\n")
		require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "INSERT")
		require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "dbr_people")
		require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "name")
		require.Equal(t, elem_count, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).finished)
		require.Equal(t, 0, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).errored)

		primaryRowsAffected, err := primaryResult.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), primaryRowsAffected)

		secondaryRowsAffected, err := secondaryResult.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), secondaryRowsAffected)

		err = txMpx.Commit()
		require.NoError(t, err)

		// Selects use only primary
		var person dbrPerson
		err = txMpx.Select("*").From("dbr_people").Where(Eq("id", id)).LoadOne(&person)
		require.Error(t, err)
		require.Equal(t, 1, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).errored)
	}
}

func TestTransactionMpxRollback(t *testing.T) {
	for _, sessMpx := range testSessionMpx {
		resetMpx(t, sessMpx)

		txMpx, err := sessMpx.Begin()
		require.NoError(t, err)
		defer txMpx.RollbackUnlessCommitted()

		if sessMpx.PrimaryConn.Dialect == dialect.MSSQL || sessMpx.SecondaryConn.Dialect == dialect.MSSQL {
			txMpx.UpdateBySql("SET IDENTITY_INSERT dbr_people ON;").Exec()
		}

		id := 1
		primaryResult, secondaryResult, err := txMpx.InsertInto("dbr_people").Columns("id", "name", "email").Values(id, "Barack", "obama@whitehouse.gov").Exec()
		require.NoError(t, err)

		primaryRowsAffected, err := primaryResult.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), primaryRowsAffected)

		secondaryRowsAffected, err := secondaryResult.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), secondaryRowsAffected)

		err = txMpx.Rollback()
		require.NoError(t, err)

		// Selects use only primary
		var person dbrPerson
		err = txMpx.Select("*").From("dbr_people").Where(Eq("id", id)).LoadOne(&person)
		require.Error(t, err)
	}
}
