package greenplum_test

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/mattn/go-sqlite3"
)

//
// Sqlmock test helpers
//

// mockDB returns a sql.DB backed by a Sqlmock controller that has been
// retrofitted with a deferrable Finish() method. Any errors are handled
// out-of-band using t.Fatalf().
func sqlmockDB(t *testing.T) (*sql.DB, *SqlmockEx) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("creating new sqlmock: %+v", err)
	}

	return db, &SqlmockEx{mock, t}
}

// SqlmockEx embeds sqlmock.Sqlmock and adds extra goodies.
type SqlmockEx struct {
	sqlmock.Sqlmock

	t *testing.T
}

// Finish is patterned on gomock's defer workflow. Call it to make sure the
// mock's expectations were met.
func (e *SqlmockEx) Finish() {
	if err := e.ExpectationsWereMet(); err != nil {
		e.t.Fatal(err)
	}
}

//
// SQLite test helpers
//

// openSQLite returns a sql.DB that connects to an in-memory SQLite 3 instance.
// This instance has a version() function that can be controlled by setting the
// value of SQLiteVersionString.
func openSQLite(t *testing.T) *sql.DB {
	db, err := sql.Open(
		"sqlite3_with_version", // see init() below

		// From the go-sqlite3 FAQ:
		//
		//   Each connection to ":memory:" opens a brand new in-memory sql
		//   database, so if the stdlib's sql engine happens to open another
		//   connection and you've only specified ":memory:", that connection
		//   will see a brand new database. A workaround is to use
		//   "file::memory:?cache=shared" (or "file:foobar?mode=memory&cache=shared").
		//   Every connection to this string will point to the same in-memory
		//   database.
		//
		"file::memory:?cache=shared",
	)
	if err != nil {
		t.Fatalf("opening sqlite instance: %+v", err)
	}

	// Also from the FAQ:
	//
	//   Note that if the last database connection in the pool closes, the
	//   in-memory database is deleted. Make sure the max idle connection limit
	//   is > 0, and the connection lifetime is infinite.
	//
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(-1)

	// XXX Fake a pg_catalog schema. SQLite doesn't use the same namespace rules
	// as Postgres, but for our current testing needs, this should mimic them
	// acceptably (i.e. implementations can refer to catalog tables as either
	// "pg_catalog.table" or "table").
	q := &queryer{t, db}
	q.MustExec("ATTACH DATABASE ':memory:' AS pg_catalog")

	return db
}

var SQLiteVersionString = "Postgres 1.2.3 (Greenplum version 4.5.6)"

func init() {
	// Register a SQLite driver with a stubbed version() function.
	sql.Register("sqlite3_with_version", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			version := func() (string, error) {
				return SQLiteVersionString, nil
			}

			return conn.RegisterFunc("version", version, false)
		},
	})
}

// queryer simplifies test error handling for database setup by providing
// MustXxx wrappers around sql.DB. Any errors are reported via t.Fatalf().
//
// Add more wrapper methods here as needed.
type queryer struct {
	t  *testing.T
	db *sql.DB
}

func (q *queryer) MustExec(query string, args ...interface{}) sql.Result {
	q.t.Helper()

	res, err := q.db.Exec(query, args...)
	if err != nil {
		q.t.Fatalf("executing query: %v", err)
	}

	return res
}
