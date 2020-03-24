package greenplum_test

import (
	"fmt"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/blang/semver"

	"github.com/greenplum-db/gpupgrade/greenplum"
)

func TestVersionFromDB(t *testing.T) {
	db, ctrl := sqlmockDB(t)
	defer ctrl.Finish()

	expectVersionQuery(ctrl, "6.4.1")

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

// expectVersionQuery sets up a Sqlmock controller to return a "real" version()
// string using the given x.y.z subversion.
func expectVersionQuery(ctrl sqlmock.Sqlmock, version string) {
	version = fmt.Sprintf("PostgreSQL 9.4.24 (Greenplum Database %s-beta.1 build dev) on x86_64-apple-darwin18.7.0, compiled by Apple clang version 11.0.0 (clang-1100.0.33.17), 64-bit compiled on Mar 11 2020 12:10:06",
		version)

	rows := sqlmock.NewRows([]string{"version"}).
		AddRow(version)

	ctrl.ExpectQuery("SELECT version()").
		WillReturnRows(rows)
}
