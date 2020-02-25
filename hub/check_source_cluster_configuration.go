package hub

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
)

const ClusterIsUp = "u" // defined by gpdb

type SegmentStatus struct {
	IsUp bool
	DbID int
}

type SegmentStatusError struct {
	DownDbids []int
}

func (e SegmentStatusError) Error() string {
	var dbidStrings []string

	for _, dbid := range e.DownDbids {
		dbidStrings = append(dbidStrings, strconv.Itoa(dbid))
	}

	message := fmt.Sprintf("Could not initialize gpupgrade. These"+
		" Greenplum segment dbids are not up: %v."+
		" Please bring all segments up before initializing.", strings.Join(dbidStrings, ", "))

	return message
}

func GetSegmentStatuses(connection *dbconn.DBConn) ([]SegmentStatus, error) {
	statuses := []SegmentStatus{}

	err := withinConnection(connection, func() error {
		results := make([]struct {
			Dbid   int
			Status string
		}, 0)

		err := connection.Select(&results,
			"select dbid as Dbid, status as Status from gp_segment_configuration")

		if err != nil {
			return err
		}

		for _, result := range results {
			statuses = append(statuses, SegmentStatus{
				IsUp: result.Status == ClusterIsUp,
				DbID: result.Dbid,
			})
		}

		return nil
	})

	return statuses, err
}

func CheckSourceClusterConfiguration(getSegmentStatuses func() ([]SegmentStatus, error)) error {
	statuses, err := getSegmentStatuses()

	if err != nil {
		return err
	}

	downSegments := filterSegments(statuses, func(status SegmentStatus) bool {
		return !status.IsUp
	})

	if len(downSegments) > 0 {
		return NewSegmentStatusError(downSegments)
	}

	return nil
}

func NewSegmentStatusError(downSegments []SegmentStatus) error {
	var downDbids []int

	for _, downSegment := range downSegments {
		downDbids = append(downDbids, downSegment.DbID)
	}

	return SegmentStatusError{downDbids}
}

func withinConnection(connection *dbconn.DBConn, operation func() error) error {
	err := connection.Connect(1)

	if err != nil {
		return errors.Wrap(err, "couldn't connect to cluster")
	}

	defer connection.Close()

	return operation()
}

func filterSegments(segments []SegmentStatus, filterMatches func(status SegmentStatus) bool) []SegmentStatus {
	var downSegments []SegmentStatus

	for _, segment := range segments {
		if filterMatches(segment) {
			downSegments = append(downSegments, segment)
		}
	}

	return downSegments
}
