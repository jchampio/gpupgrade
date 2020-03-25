package greenplum_test

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/mattn/go-sqlite3"

	"github.com/greenplum-db/gpupgrade/greenplum"
)

func TestGetSegmentConfiguration(t *testing.T) {
	db := openSQLite(t)

	expected := []greenplum.SegConfig{
		{DbID: 1, ContentID: -1, Port: 5432, Hostname: "localhost", DataDir: "/data/gpseg-1", Role: "p"},
		{DbID: 2, ContentID: 0, Port: 15432, Hostname: "localhost", DataDir: "/data/gpseg0", Role: "p"},
		{DbID: 3, ContentID: 1, Port: 15433, Hostname: "localhost", DataDir: "/data/gpseg1", Role: "p"},
		{DbID: 4, ContentID: 1, Port: 15432, Hostname: "remotehost", DataDir: "/data/mirror1", Role: "m"},
	}

	// The core of the test is the same regardless of the GPDB version: ensure
	// that we get a segment configuration that looks like the expected array
	// above.
	test := func(t *testing.T, db *sql.DB, version greenplum.Version) {
		t.Helper()

		results, err := greenplum.GetSegmentConfiguration(db, version)
		if err != nil {
			t.Errorf("returned error %+v", err)
		}

		if !reflect.DeepEqual(results, expected) {
			t.Errorf("got configuration %+v, want %+v", results, expected)
		}
	}

	t.Run("GPDB 5", func(t *testing.T) {
		q := queryer{t, db}

		// Create gp_segment_configuration. This definition is pulled from the
		// GPDB 5 catalog.
		q.MustExec("DROP TABLE IF EXISTS pg_catalog.gp_segment_configuration")
		q.MustExec(`CREATE TABLE pg_catalog.gp_segment_configuration(
			dbid int2,
			content int2,
			role char,
			preferred_role char,
			mode char,
			status char,
			port int4,
			hostname text,
			address text,
			replication_port int4
		)`)

		// Create pg_filespace similarly.
		q.MustExec("DROP TABLE IF EXISTS pg_catalog.pg_filespace")
		q.MustExec(`CREATE TABLE pg_catalog.pg_filespace(
			fsname name,
			fsowner oid
		)`)

		// Create pg_filespace_entry, which references them both.
		q.MustExec("DROP TABLE IF EXISTS pg_catalog.pg_filespace_entry")
		q.MustExec(`CREATE TABLE pg_catalog.pg_filespace_entry(
			fsefsoid oid,
			fsedbid int2,
			fselocation text
		)`)

		// Load in a sample configuration.
		q.MustExec(`INSERT INTO pg_catalog.gp_segment_configuration VALUES
			( 1, -1, 'p', 'p', 'n', 'u',  5432,  'localhost',  'localhost', 25432 ),
			( 2,  0, 'p', 'p', 'n', 'u', 15432,  'localhost',  'localhost', 25433 ),
			( 3,  1, 'p', 'p', 's', 'u', 15433,  'localhost',  'localhost', 25434 ),
			( 4,  1, 'm', 'm', 's', 'u', 15432, 'remotehost', 'remotehost', 25435 )
		`)
		q.MustExec(`INSERT INTO pg_catalog.pg_filespace VALUES
			( 'pg_system', 0 )
		`)

		datadirs := map[int]string{ // maps dbid -> datadir
			1: "/data/gpseg-1",
			2: "/data/gpseg0",
			3: "/data/gpseg1",
			4: "/data/mirror1",
		}

		// Populate the pg_filespace_entry links for each DBID. This is more
		// convoluted because we need the pg_filespace OIDs, which only the DB
		// knows at this point. (Can you see why we simplified this in 6?)
		for dbid, datadir := range datadirs {
			q.MustExec(`INSERT INTO pg_catalog.pg_filespace_entry
				SELECT oid, ?, ?
				  FROM pg_catalog.pg_filespace
				 WHERE fsname = 'pg_system'
			`, dbid, datadir)
		}

		version := dbconn.NewVersion("5.0.0")
		test(t, db, version)
	})

	t.Run("GPDB 6", func(t *testing.T) {
		q := queryer{t, db}

		// Create gp_segment_configuration. This definition is pulled from the
		// GPDB 6 catalog.
		q.MustExec("DROP TABLE IF EXISTS pg_catalog.gp_segment_configuration")
		q.MustExec(`CREATE TABLE pg_catalog.gp_segment_configuration(
			dbid int2,
			content int2,
			role char,
			preferred_role char,
			mode char,
			status char,
			port int4,
			hostname text,
			address text,
			datadir text
		)`)

		// Load in a sample configuration.
		q.MustExec(`INSERT INTO pg_catalog.gp_segment_configuration VALUES
			( 1, -1, 'p', 'p', 'n', 'u',  5432,  'localhost',  'localhost', '/data/gpseg-1' ),
			( 2,  0, 'p', 'p', 'n', 'u', 15432,  'localhost',  'localhost', '/data/gpseg0'  ),
			( 3,  1, 'p', 'p', 's', 'u', 15433,  'localhost',  'localhost', '/data/gpseg1'  ),
			( 4,  1, 'm', 'm', 's', 'u', 15432, 'remotehost', 'remotehost', '/data/mirror1' )
		`)

		version := dbconn.NewVersion("6.0.0")
		test(t, db, version)
	})
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
