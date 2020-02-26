package hub

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

const ClusterIsUp = "u" // defined by gpdb
type DbID int

type SegmentStatus struct {
	IsUp          bool
	DbID          DbID
	Role          string
	PreferredRole string
}

type UnbalancedSegmentStatusError struct {
	UnbalancedDbids []DbID
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
	DownDbids []DbID
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

func NewUnbalancedSegmentStatusError(segments []SegmentStatus) error {
	var dbids []DbID

	for _, segment := range segments {
		dbids = append(dbids, segment.DbID)
	}

	return UnbalancedSegmentStatusError{dbids}
}

func GetSegmentStatuses(connection *sql.DB) ([]SegmentStatus, error) {
	statuses := []SegmentStatus{}

	type result struct {
		Dbid          DbID
		Status        string
		Role          string
		PreferredRole string
	}

	results := make([]result, 0)

	rows, err := connection.Query("select dbid as Dbid, status as Status, role as Role, preferred_role as PreferredRole from gp_segment_configuration")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		r := result{}
		err = rows.Scan(&r.Dbid, &r.Status, &r.Role, &r.PreferredRole)
		results = append(results, r)
	}

	if err != nil {
		return nil, err
	}

	for _, result := range results {
		statuses = append(statuses, SegmentStatus{
			IsUp:          result.Status == ClusterIsUp,
			DbID:          result.Dbid,
			Role:          result.Role,
			PreferredRole: result.PreferredRole,
		})
	}

	return statuses, err
}

func CheckSourceClusterConfiguration(getSegmentStatuses func() ([]SegmentStatus, error)) error {
	var statuses []SegmentStatus

	statuses, err := getSegmentStatuses()

	if err != nil {
		return err
	}

	if err := checkForDownSegments(statuses); err != nil {
		return err
	}

	if err := checkForUnbalancedSegments(statuses); err != nil {
		return err
	}

	return nil
}

func checkForUnbalancedSegments(statuses []SegmentStatus) error {
	unbalancedSegments := filterSegments(statuses, func(status SegmentStatus) bool {
		return status.PreferredRole != status.Role
	})

	if len(unbalancedSegments) > 0 {
		return NewUnbalancedSegmentStatusError(unbalancedSegments)
	}

	return nil
}

func checkForDownSegments(statuses []SegmentStatus) error {
	downSegments := filterSegments(statuses, func(status SegmentStatus) bool {
		return !status.IsUp
	})

	if len(downSegments) > 0 {
		return NewDownSegmentStatusError(downSegments)
	}

	return nil
}

func NewDownSegmentStatusError(downSegments []SegmentStatus) error {
	var downDbids []DbID

	for _, downSegment := range downSegments {
		downDbids = append(downDbids, downSegment.DbID)
	}

	return DownSegmentStatusError{downDbids}
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
