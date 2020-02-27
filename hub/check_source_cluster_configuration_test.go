package hub_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/hub"
	"github.com/greenplum-db/gpupgrade/hub/sourcedb"
)

func TestSegmentStatusError_Error(t *testing.T) {
	t.Run("it formats a list of dbids", func(t *testing.T) {
		err := hub.DownSegmentStatusError{DownDbids: []sourcedb.DBID{1, 2, 3}}

		if !strings.Contains(err.Error(), "1, 2, 3") {
			t.Errorf("got %v, expected dbids to be included", err.Error())
		}
	})
}

func TestUnbalancedSegmentStatusError_Error(t *testing.T) {
	t.Run("it formats a list of dbids", func(t *testing.T) {
		err := hub.UnbalancedSegmentStatusError{UnbalancedDbids: []sourcedb.DBID{1, 2, 3}}

		if !strings.Contains(err.Error(), "1, 2, 3") {
			t.Errorf("got %v, expected dbids to be included", err.Error())
		}
	})
}

func TestCheckSourceClusterConfiguration(t *testing.T) {
	t.Run("it passes when when all segments are up", func(t *testing.T) {
		sourceDatabase := &mockSourceDatabase{
			func(sourcedb.Database) ([]sourcedb.SegmentStatus, error) {
				return []sourcedb.SegmentStatus{
					{DbID: 0, IsUp: true},
					{DbID: 1, IsUp: true},
					{DbID: 2, IsUp: true},
				}, nil
			},
		}

		err := hub.CheckSourceClusterConfiguration(sourceDatabase)

		if err != nil {
			t.Errorf("got no completion message, expected substep to complete without errors")
		}
	})

	t.Run("it returns an error if it fails to query for statuses", func(t *testing.T) {
		queryError := errors.New("some error while querying")

		sourceDatabase := mockSourceDatabase{func(sourcedb.Database) ([]sourcedb.SegmentStatus, error) {
			return []sourcedb.SegmentStatus{}, queryError
		}}

		err := hub.CheckSourceClusterConfiguration(sourceDatabase)

		if err == nil {
			t.Fatalf("got no error, expected an error")
		}

		if !strings.Contains(err.Error(), queryError.Error()) {
			t.Errorf("got %q, expected an error to get bubbled up from the failed query %q",
				err, queryError)
		}
	})

	t.Run("it returns an error if any of the segments are not in their preferred role", func(t *testing.T) {
		sourceDatabase := mockSourceDatabase{func(sourcedb.Database) ([]sourcedb.SegmentStatus, error) {
			return []sourcedb.SegmentStatus{
				makeBalanced(1),
				makeUnbalanced(2),
				makeBalanced(3),
				makeUnbalanced(4),
			}, nil
		}}

		err := hub.CheckSourceClusterConfiguration(sourceDatabase)

		if err == nil {
			t.Fatalf("got no errors for step, expected segment status error")
		}

		var multiError *multierror.Error
		var segmentStatusError hub.UnbalancedSegmentStatusError

		if !xerrors.As(err, &multiError) || !xerrors.As(multiError.Errors[0], &segmentStatusError) {
			t.Errorf("got an error that was not a segment status error: %v",
				err.Error())
		}

		unbalancedListIncludes := func(expectedDbid sourcedb.DBID) bool {
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
		sourceDatabase := mockSourceDatabase{func(sourcedb.Database) ([]sourcedb.SegmentStatus, error) {
			return []sourcedb.SegmentStatus{
				{DbID: 0, IsUp: true},
				{DbID: 1, IsUp: false},
				{DbID: 2, IsUp: true},
				{DbID: 99, IsUp: false},
			}, nil
		}}

		err := hub.CheckSourceClusterConfiguration(sourceDatabase)

		if err == nil {
			t.Fatalf("got no errors for step, expected segment status error")
		}

		var segmentStatusError hub.DownSegmentStatusError

		var multiError *multierror.Error
		if !xerrors.As(err, &multiError) || !xerrors.As(multiError.Errors[0], &segmentStatusError) {
			t.Errorf("got an error that was not a segment status error: %v",
				err.Error())
		}

		downListIncludes := func(expectedDbid sourcedb.DBID) bool {
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

	t.Run("it returns both unbalanced errors and down errors at the same time", func(t *testing.T) {
		sourceDatabase := mockSourceDatabase{func(sourcedb.Database) ([]sourcedb.SegmentStatus, error) {
			return []sourcedb.SegmentStatus{
				{DbID: 1, IsUp: false},
				makeUnbalanced(2),
			}, nil
		}}

		err := hub.CheckSourceClusterConfiguration(sourceDatabase)

		if err == nil {
			t.Fatalf("got no errors for step, expected segment status error")
		}

		var multiError *multierror.Error

		if !xerrors.As(err, &multiError) {
			t.Errorf("got an error that was not a multi error: %v",
				err.Error())
		}

		var downSegmentStatusError hub.DownSegmentStatusError
		if !xerrors.As(multiError.Errors[0], &downSegmentStatusError) {
			t.Errorf("got an error that was not a down segment status error: %v",
				err.Error())
		}

		var unbalancedSegmentStatusError hub.UnbalancedSegmentStatusError
		if !xerrors.As(multiError.Errors[1], &unbalancedSegmentStatusError) {
			t.Errorf("got an error that was not an unbalanced segment status error: %v",
				err.Error())
		}
	})
}

func makeBalanced(dbid sourcedb.DBID) sourcedb.SegmentStatus {
	return sourcedb.SegmentStatus{
		IsUp:          true,
		DbID:          dbid,
		Role:          sourcedb.Primary,
		PreferredRole: sourcedb.Primary,
	}
}

func makeUnbalanced(dbid sourcedb.DBID) sourcedb.SegmentStatus {
	return sourcedb.SegmentStatus{
		IsUp:          true,
		DbID:          dbid,
		Role:          sourcedb.Mirror,
		PreferredRole: sourcedb.Primary,
	}
}

type mockSourceDatabase struct {
	getSegmentStatuses func(sourcedb.Database) ([]sourcedb.SegmentStatus, error)
}

func (m mockSourceDatabase) GetSegmentStatuses() ([]sourcedb.SegmentStatus, error) {
	return m.getSegmentStatuses(m)
}
