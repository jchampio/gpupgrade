package hub_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/greenplum-db/gpupgrade/utils"

	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/hub"
)

func TestSegmentStatusError_Error(t *testing.T) {
	t.Run("it formats a list of dbids", func(t *testing.T) {
		err := hub.DownSegmentStatusError{DownDbids: []hub.DbID{1, 2, 3}}

		if !strings.Contains(err.Error(), "1, 2, 3") {
			t.Errorf("got %v, expected dbids to be included", err.Error())
		}
	})
}

func TestUnbalancedSegmentStatusError_Error(t *testing.T) {
	t.Run("it formats a list of dbids", func(t *testing.T) {
		err := hub.UnbalancedSegmentStatusError{UnbalancedDbids: []hub.DbID{1, 2, 3}}

		if !strings.Contains(err.Error(), "1, 2, 3") {
			t.Errorf("got %v, expected dbids to be included", err.Error())
		}
	})
}

func TestCheckSourceClusterConfiguration(t *testing.T) {
	t.Run("it passes when when all segments are up", func(t *testing.T) {
		stubGetSegmentStatus := func() ([]hub.SegmentStatus, error) {
			return []hub.SegmentStatus{
				{DbID: 0, IsUp: true},
				{DbID: 1, IsUp: true},
				{DbID: 2, IsUp: true},
			}, nil
		}

		err := hub.CheckSourceClusterConfiguration(stubGetSegmentStatus)

		if err != nil {
			t.Errorf("got no completion message, expected substep to complete without errors")
		}
	})

	t.Run("it returns an error if it fails to query for statuses", func(t *testing.T) {
		queryError := errors.New("some error while querying")

		err := hub.CheckSourceClusterConfiguration(func() ([]hub.SegmentStatus, error) {
			return []hub.SegmentStatus{}, queryError
		})

		if err == nil {
			t.Fatalf("got no error, expected an error")
		}

		if !strings.Contains(err.Error(), queryError.Error()) {
			t.Errorf("got %q, expected an error to get bubbled up from the failed query %q",
				err, queryError)
		}
	})

	t.Run("it returns an error if any of the segments are not in their preferred role", func(t *testing.T) {
		err := hub.CheckSourceClusterConfiguration(func() ([]hub.SegmentStatus, error) {
			return []hub.SegmentStatus{
				makeBalanced(1),
				makeUnbalanced(2),
				makeBalanced(3),
				makeUnbalanced(4),
			}, nil
		})

		if err == nil {
			t.Fatalf("got no errors for step, expected segment status error")
		}

		var segmentStatusError hub.UnbalancedSegmentStatusError

		if !xerrors.As(err, &segmentStatusError) {
			t.Errorf("got an error that was not a segment status error: %v",
				err.Error())
		}

		unbalancedListIncludes := func(expectedDbid hub.DbID) bool {
			for _, dbid := range segmentStatusError.UnbalancedDbids {
				if dbid == expectedDbid {
					return true
				}
			}

			return false
		}

		if !unbalancedListIncludes(2) {
			t.Errorf("got unbalanced dbids of %v, expected list to include %v",
				segmentStatusError.UnbalancedDbids,
				2)
		}

		if !unbalancedListIncludes(4) {
			t.Errorf("got unbalanced dbids of %v, expected list to include %v",
				segmentStatusError.UnbalancedDbids,
				4)
		}

		if unbalancedListIncludes(1) {
			t.Errorf("got unbalanced dbids of %v, expected list NOT TO include %v",
				segmentStatusError.UnbalancedDbids,
				1)
		}

		if unbalancedListIncludes(3) {
			t.Errorf("got down dbids of %v, expected list NOT TO include %v",
				segmentStatusError.UnbalancedDbids,
				3)
		}

	})

	t.Run("it returns an error if any of the segments are down", func(t *testing.T) {
		err := hub.CheckSourceClusterConfiguration(func() ([]hub.SegmentStatus, error) {
			return []hub.SegmentStatus{
				{DbID: 0, IsUp: true},
				{DbID: 1, IsUp: false},
				{DbID: 2, IsUp: true},
				{DbID: 99, IsUp: false},
			}, nil
		})

		if err == nil {
			t.Fatalf("got no errors for step, expected segment status error")
		}

		var segmentStatusError hub.DownSegmentStatusError

		if !xerrors.As(err, &segmentStatusError) {
			t.Errorf("got an error that was not a segment status error: %v",
				err.Error())
		}

		downListIncludes := func(expectedDbid hub.DbID) bool {
			for _, dbid := range segmentStatusError.DownDbids {
				if dbid == expectedDbid {
					return true
				}
			}

			return false
		}

		if !downListIncludes(1) {
			t.Errorf("got down dbids of %v, expected list to include %v",
				segmentStatusError.DownDbids,
				1)
		}

		if !downListIncludes(99) {
			t.Errorf("got down dbids of %v, expected list to include %v",
				segmentStatusError.DownDbids,
				99)
		}

		if downListIncludes(0) {
			t.Errorf("got down dbids of %v, expected list NOT TO include %v",
				segmentStatusError.DownDbids,
				0)
		}
	})
}

func TestGetSegmentStatuses(t *testing.T) {
	t.Run("it returns segment statuses", func(t *testing.T) {
		connection, sqlmock, err := sqlmock.New()

		rows := sqlmock.
			NewRows([]string{"Dbid", "Status", "Role", "PreferredRole"}).
			AddRow("1", "u", "m", "p").
			AddRow("2", "d", "p", "m")

		query := "select dbid as Dbid, status as Status, role as Role, preferred_role as PreferredRole from gp_segment_configuration"

		sqlmock.ExpectQuery(query).WillReturnRows(rows)

		statuses, err := hub.GetSegmentStatuses(connection)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(statuses) != 2 {
			t.Errorf("got %d rows, expected 2 rows to be returned", len(statuses))
		}

		first := statuses[0]
		if first.DbID != 1 || first.IsUp != true || first.Role != utils.MirrorRole || first.PreferredRole != utils.PrimaryRole {
			t.Errorf("segment status not populated correctly: %+v", first)
		}

		second := statuses[1]
		if second.DbID != 2 || second.IsUp != false || second.Role != utils.PrimaryRole || second.PreferredRole != utils.MirrorRole {
			t.Errorf("segment status not populated correctly: %+v", second)
		}
	})
}

func makeBalanced(dbid hub.DbID) hub.SegmentStatus {
	return hub.SegmentStatus{
		IsUp:          true,
		DbID:          dbid,
		Role:          utils.PrimaryRole,
		PreferredRole: utils.PrimaryRole,
	}
}

func makeUnbalanced(dbid hub.DbID) hub.SegmentStatus {
	return hub.SegmentStatus{
		IsUp:          true,
		DbID:          dbid,
		Role:          utils.MirrorRole,
		PreferredRole: utils.PrimaryRole,
	}
}
