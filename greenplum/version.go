package greenplum

import (
	"database/sql"
	"regexp"
	"strings"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"golang.org/x/xerrors"
)

// Version contains the GPDB version as both a convenience string and a semver.
// TODO: copy this from dbconn and break the dependency entirely
type Version = dbconn.GPDBVersion

// VersionFromDB creates a new Version matching the provided database. The
// version() function is used to find this information; a valid Greenplum
// version string is required.
func VersionFromDB(db *sql.DB) (Version, error) {
	var v Version
	var err error

	v.VersionString, err = queryVersion(db)
	if err != nil {
		return v, err
	}

	// Find the semver inside the GPDB-specific format. (The first thing that
	// looks like a semver is the PostgreSQL version, which we want to ignore
	// for these purposes.)
	const prefix = "(Greenplum Database "

	start := strings.Index(v.VersionString, prefix) + len(prefix)
	end := strings.Index(v.VersionString, ")")
	v.VersionString = v.VersionString[start:end]

	threeDigitVersion := versionPattern.FindStringSubmatch(v.VersionString)[0]
	v.SemVer, err = semver.Make(threeDigitVersion)

	return v, err
}

func queryVersion(db *sql.DB) (string, error) {
	var version string

	rows, err := db.Query("SELECT version()")
	if err != nil {
		return "", xerrors.Errorf("querying version(): %w", err)
	}

	for rows.Next() {
		if err := rows.Scan(&version); err != nil {
			return "", xerrors.Errorf("scanning version(): %w", err)
		}

		rows.Close() // we expect only one row
	}

	if rows.Err() != nil {
		return "", xerrors.Errorf("iterating version(): %w", err)
	}

	return version, nil
}

// versionPattern is used to find a three-part semver inside a GPDB version
// string.
var versionPattern *regexp.Regexp

func init() {
	versionPattern = regexp.MustCompile(`\d+\.\d+\.\d+`)
}
