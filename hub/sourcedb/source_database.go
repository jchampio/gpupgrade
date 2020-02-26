package sourcedb

import (
	"database/sql"
)

//
// Source Database:
//
// to be used before we have a Source and Target cluster
//
//
type Database interface {
	GetSegmentStatuses() ([]SegmentStatus, error)
}

type database struct {
	connection *sql.DB
}

func Initialize() *database {
	return &database{}
}
