package hub

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/hub/sourcedb"
)

type UnbalancedSegmentStatusError struct {
	UnbalancedDbids []sourcedb.DBID
}

func (e UnbalancedSegmentStatusError) Error() string {
	var dbidStrings []string

	for _, dbid := range e.UnbalancedDbids {
		dbidStrings = append(dbidStrings, strconv.Itoa(int(dbid)))
	}

	message := fmt.Sprintf("Could not initialize gpupgrade. These"+
		" Greenplum segment dbids are not in their preferred role: %v."+
		" Run gprecoverseg -r to rebalance the cluster.", strings.Join(dbidStrings, ", "))

	return message
}

type DownSegmentStatusError struct {
	DownDbids []sourcedb.DBID
}

func NewDownSegmentStatusError(downSegments []sourcedb.SegmentStatus) error {
	var downDbids []sourcedb.DBID

	for _, downSegment := range downSegments {
		downDbids = append(downDbids, downSegment.DbID)
	}

	return DownSegmentStatusError{downDbids}
}

func (e DownSegmentStatusError) Error() string {
	var dbidStrings []string

	for _, dbid := range e.DownDbids {
		dbidStrings = append(dbidStrings, strconv.Itoa(int(dbid)))
	}

	message := fmt.Sprintf("Could not initialize gpupgrade. These"+
		" Greenplum segment dbids are not up: %v."+
		" Please bring all segments up before initializing.", strings.Join(dbidStrings, ", "))

	return message
}

func NewUnbalancedSegmentStatusError(segments []sourcedb.SegmentStatus) error {
	var dbids []sourcedb.DBID

	for _, segment := range segments {
		dbids = append(dbids, segment.DbID)
	}

	return UnbalancedSegmentStatusError{dbids}
}

func CheckSourceClusterConfiguration(db sourcedb.Database) error {
	errors := &multierror.Error{}

	statuses, err := db.GetSegmentStatuses()

	if err != nil {
		return err
	}

	if err := checkForDownSegments(statuses); err != nil {
		errors = multierror.Append(errors, err)
	}

	if err := checkForUnbalancedSegments(statuses); err != nil {
		errors = multierror.Append(errors, err)
	}

	return errors.ErrorOrNil()
}

func checkForUnbalancedSegments(statuses []sourcedb.SegmentStatus) error {
	unbalancedSegments := filterSegments(statuses, func(status sourcedb.SegmentStatus) bool {
		return status.PreferredRole != status.Role
	})

	if len(unbalancedSegments) > 0 {
		return NewUnbalancedSegmentStatusError(unbalancedSegments)
	}

	return nil
}

func checkForDownSegments(statuses []sourcedb.SegmentStatus) error {
	downSegments := filterSegments(statuses, func(status sourcedb.SegmentStatus) bool {
		return !status.IsUp
	})

	if len(downSegments) > 0 {
		return NewDownSegmentStatusError(downSegments)
	}

	return nil
}

func filterSegments(segments []sourcedb.SegmentStatus, filterMatches func(status sourcedb.SegmentStatus) bool) []sourcedb.SegmentStatus {
	var downSegments []sourcedb.SegmentStatus

	for _, segment := range segments {
		if filterMatches(segment) {
			downSegments = append(downSegments, segment)
		}
	}

	return downSegments
}
