package greenplum_test

import (
	"database/sql"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver"

	"github.com/greenplum-db/gpupgrade/greenplum"
)

func TestVersionFromDB(t *testing.T) {
	db, ctrl := mockDB(t)
	defer ctrl.Finish()

	versionRows := sqlmock.NewRows([]string{"version"}).
		AddRow("PostgreSQL 9.4.24 (Greenplum Database 6.4.1-beta.1 build dev) on x86_64-apple-darwin18.7.0, compiled by Apple clang version 11.0.0 (clang-1100.0.33.17), 64-bit compiled on Mar 11 2020 12:10:06")

	ctrl.ExpectQuery("SELECT version()").
		WillReturnRows(versionRows)

	version, err := greenplum.VersionFromDB(db)
	if err != nil {
		t.Errorf("returned error %+v", err)
	}

	expected := "6.4.1-beta.1 build dev"
	if version.VersionString != expected {
		t.Errorf("version string was %q, want %q", version.VersionString, expected)
	}

	expectedVersion := semver.MustParse("6.4.1")
	if !version.SemVer.Equals(expectedVersion) {
		t.Errorf("semver was %s, want %s", version.SemVer, expectedVersion)
	}

	// TODO: flesh out this test to get more coverage.
}

func mockDB(t *testing.T) (*sql.DB, *SqlmockEx) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("creating new sqlmock: %+v", err)
	}

	return db, &SqlmockEx{mock, t}
}

// SqlmockEx has some extra goodies compared to Sqlmock.
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
