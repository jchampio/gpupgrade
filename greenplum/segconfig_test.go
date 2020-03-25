package greenplum_test

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"

	"github.com/greenplum-db/gpupgrade/greenplum"
)

func TestGetSegmentConfiguration(t *testing.T) {
	db := openSQLite(t)
	defer db.Close()

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
