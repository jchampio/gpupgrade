package hub

import (
	"testing"

	"github.com/golang/mock/gomock"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/idl/mock_idl"
)

func TestSampleThing(t *testing.T) {
	// The expected request is identical across all calls for this sample.
	ratio := 0.5
	expectedRequest := &idl.CheckSegmentDiskSpaceRequest{
		Request: &idl.CheckDiskSpaceRequest{
			Ratio: ratio,
		},
		Datadirs: []string{},
	}

	t.Run("calls CheckDiskSpace on each agent", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// Build our list of mock connections.
		var conns []*Connection
		for i := 0; i < 4; i++ {
			mockConn := mock_idl.NewMockAgentClient(ctrl)
			mockConn.EXPECT().CheckDiskSpace(
				gomock.Any(),
				expectedRequest,
			).Times(1)

			conns = append(conns, &Connection{
				AgentClient: mockConn,
			})
		}

		// Test.
		err := SampleThing(conns, ratio)
		if err != nil {
			t.Errorf("returned error %+v", err)
		}
	})

	t.Run("reports errors from each agent", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		expectedErr := errors.New("failed connection")

		// Build our list of mock connections.
		var conns []*Connection
		for i := 0; i < 4; i++ {
			mockConn := mock_idl.NewMockAgentClient(ctrl)
			mockConn.EXPECT().CheckDiskSpace(
				gomock.Any(),
				expectedRequest,
			).Return(
				&idl.CheckDiskSpaceReply{},
				expectedErr,
			).Times(1)

			conns = append(conns, &Connection{
				AgentClient: mockConn,
			})
		}

		// Test.
		err := SampleThing(conns, ratio)

		var multierr *multierror.Error
		if !xerrors.As(err, &multierr) {
			t.Fatalf("returned error %#v, want type %T", err, multierr)
		}

		if len(multierr.Errors) != len(conns) {
			t.Errorf("returned %d errors, want %d", len(multierr.Errors), len(conns))
		}

		for _, err := range multierr.Errors {
			if !xerrors.Is(err, expectedErr) {
				t.Errorf("returned error %#v, want %#v", err, expectedErr)
			}
		}
	})
}
