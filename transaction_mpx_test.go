package dbr

import (
	"context"
	"golang.org/x/sync/errgroup"
	"testing"

	"github.com/gocraft/dbr/v2/dialect"
	"github.com/stretchr/testify/require"
)

func TestTransactionCommitMpx(t *testing.T) {
	for _, sessMpx := range testSessionMpx {
		resetMpx(t, sessMpx)

		primaryTx, secondaryAsyncTxChan, err := sessMpx.Begin()
		require.NoError(t, err)
		defer primaryTx.RollbackUnlessCommitted()

		elem_count := 1
		if sessMpx.PrimaryConn.Dialect == dialect.MSSQL {
			primaryTx.UpdateBySql("SET IDENTITY_INSERT dbr_people ON;").Exec()
			elem_count += 1
		}

		id := 1
		primaryResult, err := primaryTx.InsertInto("dbr_people").Columns("id", "name", "email").Values(id, "Barack", "obama@whitehouse.gov").Comment("INSERT TEST").Exec()
		require.NoError(t, err)

		require.Len(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started, elem_count)

		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].eventName, "dbr.exec")
		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "/* INSERT TEST */\n")
		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "INSERT")
		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "dbr_people")
		require.Contains(t, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).started[elem_count-1].query, "name")
		require.Equal(t, elem_count, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).finished)
		require.Equal(t, 0, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).errored)

		primaryRowsAffected, err := primaryResult.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), primaryRowsAffected)

		err = primaryTx.Commit()
		require.NoError(t, err)

		// Selects use only primary
		var person dbrPerson
		err = primaryTx.Select("*").From("dbr_people").Where(Eq("id", id)).LoadOne(&person)
		require.Error(t, err)
		require.Equal(t, 1, sessMpx.PrimaryEventReceiver.(*testTraceReceiver).errored)

		eg, egCtx := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			for {
				select {
				case <-egCtx.Done():
					return egCtx.Err()
				case asyncTx, ok := <-secondaryAsyncTxChan:
					require.True(t, ok)

					for {
						select {
						case <-egCtx.Done():
							return egCtx.Err()
						case rerr, ok := <-asyncTx.ErrChan:
							require.True(t, ok)
							require.NoError(t, rerr)
						default:
							secondaryTx := asyncTx.Tx
							defer secondaryTx.RollbackUnlessCommitted()

							secondaryElemCount := 1
							if sessMpx.SecondaryConn.Dialect == dialect.MSSQL {
								secondaryTx.UpdateBySql("SET IDENTITY_INSERT dbr_people ON;").Exec()
								secondaryElemCount += 1
							}

							secondaryResult, rerr := secondaryTx.InsertInto("dbr_people").Columns("id", "name", "email").Values(id, "Barack", "obama@whitehouse.gov").Comment("INSERT TEST").Exec()
							require.NoError(t, rerr)

							require.Len(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started, secondaryElemCount)

							require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[secondaryElemCount-1].eventName, "dbr.exec")
							require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[secondaryElemCount-1].query, "/* INSERT TEST */\n")
							require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[secondaryElemCount-1].query, "INSERT")
							require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[secondaryElemCount-1].query, "dbr_people")
							require.Contains(t, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).started[secondaryElemCount-1].query, "name")
							require.Equal(t, secondaryElemCount, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).finished)
							require.Equal(t, 0, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).errored)

							secondaryRowsAffected, rerr := secondaryResult.RowsAffected()
							require.NoError(t, rerr)
							require.Equal(t, int64(1), secondaryRowsAffected)

							rerr = secondaryTx.Commit()
							require.NoError(t, rerr)

							var person dbrPerson
							rerr = secondaryTx.Select("*").From("dbr_people").Where(Eq("id", id)).LoadOne(&person)
							require.Error(t, rerr)
							require.Equal(t, 1, sessMpx.SecondaryEventReceiver.(*testTraceReceiver).errored)

							return nil
						}
					}
				}
			}
		})

		err = eg.Wait()
		require.NoError(t, err)
	}
}

func TestTransactionMpxRollback(t *testing.T) {
	for _, sessMpx := range testSessionMpx {
		resetMpx(t, sessMpx)

		primaryTx, secondaryAsyncTxChan, err := sessMpx.Begin()
		require.NoError(t, err)
		defer primaryTx.RollbackUnlessCommitted()

		if sessMpx.PrimaryConn.Dialect == dialect.MSSQL {
			primaryTx.UpdateBySql("SET IDENTITY_INSERT dbr_people ON;").Exec()
		}

		id := 1
		primaryResult, err := primaryTx.InsertInto("dbr_people").Columns("id", "name", "email").Values(id, "Barack", "obama@whitehouse.gov").Exec()
		require.NoError(t, err)

		primaryRowsAffected, err := primaryResult.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), primaryRowsAffected)

		err = primaryTx.Rollback()
		require.NoError(t, err)

		// Selects use only primary
		var person dbrPerson
		err = primaryTx.Select("*").From("dbr_people").Where(Eq("id", id)).LoadOne(&person)
		require.Error(t, err)

		eg, egCtx := errgroup.WithContext(context.Background())
		eg.Go(func() error {
			for {
				select {
				case <-egCtx.Done():
					return egCtx.Err()
				case asyncTx, ok := <-secondaryAsyncTxChan:
					require.True(t, ok)

					for {
						select {
						case <-egCtx.Done():
							return egCtx.Err()
						case rerr, ok := <-asyncTx.ErrChan:
							require.True(t, ok)
							require.NoError(t, rerr)
						default:
							secondaryTx := asyncTx.Tx
							defer secondaryTx.RollbackUnlessCommitted()

							if sessMpx.SecondaryConn.Dialect == dialect.MSSQL {
								primaryTx.UpdateBySql("SET IDENTITY_INSERT dbr_people ON;").Exec()
							}

							secondaryResult, rerr := secondaryTx.InsertInto("dbr_people").Columns("id", "name", "email").Values(id, "Barack", "obama@whitehouse.gov").Exec()
							require.NoError(t, rerr)

							secondaryRowsAffected, rerr := secondaryResult.RowsAffected()
							require.NoError(t, rerr)
							require.Equal(t, int64(1), secondaryRowsAffected)

							rerr = secondaryTx.Rollback()
							require.NoError(t, rerr)

							var person dbrPerson
							rerr = secondaryTx.Select("*").From("dbr_people").Where(Eq("id", id)).LoadOne(&person)
							require.Error(t, rerr)

							return nil
						}
					}
				}
			}
		})
	}
}
