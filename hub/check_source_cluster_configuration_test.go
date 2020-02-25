package hub_test

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/hub"
)

func TestSegmentStatusError_Error(t *testing.T) {
	t.Run("it formats a list of dbids", func(t *testing.T) {
		err := hub.SegmentStatusError{DownDbids: []int{1, 2, 3}}

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
			t.Errorf("got no failed messages, expected failed substep")
			t.Fatalf("got no errors for step, expected segment status error")
		}

		var segmentStatusError hub.SegmentStatusError

		if !xerrors.As(err, &segmentStatusError) {
			t.Errorf("got an error that was not a segment status error: %v",
				err.Error())
		}

		downListIncludes := func(expectedDbid int) bool {
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
