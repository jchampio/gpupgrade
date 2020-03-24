package greenplum

import (
	"database/sql"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

type SegConfig struct {
	DbID      int
	ContentID int
	Port      int
	Hostname  string
	DataDir   string
	Role      string
}

const (
	PrimaryRole = "p"
	MirrorRole  = "m"
)

func (s *SegConfig) IsMaster() bool {
	return s.ContentID == -1 && s.Role == "p"
}

func (s *SegConfig) IsStandby() bool {
	return s.ContentID == -1 && s.Role == "m"
}

func GetSegmentConfiguration(db *sql.DB, version Version) ([]SegConfig, error) {
	query := ""
	if version.Before("6") {
		query = `
SELECT
	s.dbid,
	s.content as contentid,
	s.port,
	s.hostname,
	e.fselocation as datadir,
	s.role
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE f.fsname = 'pg_system'
ORDER BY s.content;`
	} else {
		query = `
SELECT
	dbid,
	content as contentid,
	port,
	hostname,
	datadir,
	role
FROM gp_segment_configuration
ORDER BY content;`
	}

	var results []SegConfig

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var seg SegConfig

		err := rows.Scan(&seg.DbID, &seg.ContentID, &seg.Port, &seg.Hostname, &seg.DataDir, &seg.Role)
		if err != nil {
			return nil, err
		}

		results = append(results, seg)
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return results, nil
}

func MustGetSegmentConfiguration(db *sql.DB, version Version) []SegConfig {
	segConfigs, err := GetSegmentConfiguration(db, version)
	gplog.FatalOnError(err)
	return segConfigs
}
