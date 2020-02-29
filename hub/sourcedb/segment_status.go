package sourcedb

import "database/sql"

type DBID int
type Role string

const (
	Primary Role = "p"
	Mirror       = "m"
)

type Status string

const (
	Up   Status = "u"
	Down        = "d"
)

type SegmentStatus struct {
	IsUp          bool
	DbID          DBID
	Role          Role
	PreferredRole Role
}

func GetSegmentStatuses(connection *sql.DB) ([]SegmentStatus, error) {
	statuses := make([]SegmentStatus, 0)

	rows, err := connection.Query(`
		select dbid, status = $1 as is_up, role, preferred_role
		from gp_segment_configuration
	`, Up)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		r := SegmentStatus{}
		err = rows.Scan(&r.DbID, &r.IsUp, &r.Role, &r.PreferredRole)
		statuses = append(statuses, r)
	}

	if err != nil {
		return nil, err
	}

	return statuses, err
}
