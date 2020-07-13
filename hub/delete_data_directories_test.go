// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub_test

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/hub"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/idl/mock_idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/testutils"
)

func TestDeleteSegmentDataDirs(t *testing.T) {
	c := hub.MustCreateCluster(t, []greenplum.SegConfig{
		{ContentID: -1, DbID: 0, Port: 25431, Hostname: "master", DataDir: "/data/qddir", Role: greenplum.PrimaryRole},
		{ContentID: -1, DbID: 1, Port: 25431, Hostname: "standby", DataDir: "/data/standby", Role: greenplum.MirrorRole},
		{ContentID: 0, DbID: 2, Port: 25432, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
		{ContentID: 1, DbID: 3, Port: 25433, Hostname: "sdw2", DataDir: "/data/dbfast2/seg2", Role: greenplum.PrimaryRole},
		{ContentID: 2, DbID: 4, Port: 25434, Hostname: "sdw1", DataDir: "/data/dbfast1/seg3", Role: greenplum.PrimaryRole},
		{ContentID: 3, DbID: 5, Port: 25435, Hostname: "sdw2", DataDir: "/data/dbfast2/seg4", Role: greenplum.PrimaryRole},
		{ContentID: 0, DbID: 6, Port: 35432, Hostname: "sdw1", DataDir: "/data/dbfast_mirror1/seg1", Role: greenplum.MirrorRole},
		{ContentID: 1, DbID: 7, Port: 35433, Hostname: "sdw2", DataDir: "/data/dbfast_mirror2/seg2", Role: greenplum.MirrorRole},
		{ContentID: 2, DbID: 8, Port: 35434, Hostname: "sdw1", DataDir: "/data/dbfast_mirror1/seg3", Role: greenplum.MirrorRole},
		{ContentID: 3, DbID: 9, Port: 35435, Hostname: "sdw2", DataDir: "/data/dbfast_mirror2/seg4", Role: greenplum.MirrorRole},
	})

	testhelper.SetupTestLogger()

	t.Run("DeleteMirrorAndStandbyDataDirectories", func(t *testing.T) {
		t.Run("deletes standby and mirror data directories", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			sdw1Client := mock_idl.NewMockAgentClient(ctrl)
			sdw1Client.EXPECT().DeleteDataDirectories(
				gomock.Any(),
				&idl.DeleteDataDirectoriesRequest{Datadirs: []string{
					"/data/dbfast_mirror1/seg1",
					"/data/dbfast_mirror1/seg3",
				}},
			).Return(&idl.DeleteDataDirectoriesReply{}, nil)

			sdw2Client := mock_idl.NewMockAgentClient(ctrl)
			sdw2Client.EXPECT().DeleteDataDirectories(
				gomock.Any(),
				&idl.DeleteDataDirectoriesRequest{Datadirs: []string{
					"/data/dbfast_mirror2/seg2",
					"/data/dbfast_mirror2/seg4",
				}},
			).Return(&idl.DeleteDataDirectoriesReply{}, nil)

			standbyClient := mock_idl.NewMockAgentClient(ctrl)
			standbyClient.EXPECT().DeleteDataDirectories(
				gomock.Any(),
				&idl.DeleteDataDirectoriesRequest{Datadirs: []string{"/data/standby"}},
			).Return(&idl.DeleteDataDirectoriesReply{}, nil)

			agentConns := []*hub.Connection{
				{nil, sdw1Client, "sdw1", nil},
				{nil, sdw2Client, "sdw2", nil},
				{nil, standbyClient, "standby", nil},
			}

			err := hub.DeleteMirrorAndStandbyDataDirectories(agentConns, c)
			if err != nil {
				t.Errorf("unexpected err %#v", err)
			}
		})
	})

	t.Run("DeletePrimaryDataDirectories", func(t *testing.T) {
		t.Run("deletes primary data directories", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			sdw1Client := mock_idl.NewMockAgentClient(ctrl)
			sdw1Client.EXPECT().DeleteDataDirectories(
				gomock.Any(),
				&idl.DeleteDataDirectoriesRequest{Datadirs: []string{
					"/data/dbfast1/seg1",
					"/data/dbfast1/seg3",
				}},
			).Return(&idl.DeleteDataDirectoriesReply{}, nil)

			sdw2Client := mock_idl.NewMockAgentClient(ctrl)
			sdw2Client.EXPECT().DeleteDataDirectories(
				gomock.Any(),
				&idl.DeleteDataDirectoriesRequest{Datadirs: []string{
					"/data/dbfast2/seg2",
					"/data/dbfast2/seg4",
				}},
			).Return(&idl.DeleteDataDirectoriesReply{}, nil)

			standbyClient := mock_idl.NewMockAgentClient(ctrl)
			// NOTE: we expect no call to the standby

			agentConns := []*hub.Connection{
				{nil, sdw1Client, "sdw1", nil},
				{nil, sdw2Client, "sdw2", nil},
				{nil, standbyClient, "standby", nil},
			}

			err := hub.DeletePrimaryDataDirectories(agentConns, c)
			if err != nil {
				t.Errorf("unexpected err %#v", err)
			}
		})

		t.Run("returns error on failure", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			sdw1Client := mock_idl.NewMockAgentClient(ctrl)
			sdw1Client.EXPECT().DeleteDataDirectories(
				gomock.Any(),
				gomock.Any(),
			).Return(&idl.DeleteDataDirectoriesReply{}, nil)

			expected := errors.New("permission denied")
			sdw2ClientFailed := mock_idl.NewMockAgentClient(ctrl)
			sdw2ClientFailed.EXPECT().DeleteDataDirectories(
				gomock.Any(),
				gomock.Any(),
			).Return(nil, expected)

			agentConns := []*hub.Connection{
				{nil, sdw1Client, "sdw1", nil},
				{nil, sdw2ClientFailed, "sdw2", nil},
			}

			err := hub.DeletePrimaryDataDirectories(agentConns, c)

			var multiErr *multierror.Error
			if !xerrors.As(err, &multiErr) {
				t.Fatalf("got error %#v, want type %T", err, multiErr)
			}

			if len(multiErr.Errors) != 1 {
				t.Errorf("received %d errors, want %d", len(multiErr.Errors), 1)
			}

			for _, err := range multiErr.Errors {
				if !xerrors.Is(err, expected) {
					t.Errorf("got error %#v, want %#v", expected, err)
				}
			}
		})
	})
}

func TestDeleteTablespaceDirectories(t *testing.T) {
	target := hub.MustCreateCluster(t, []greenplum.SegConfig{
		{DbID: 1, ContentID: -1, Hostname: "master", DataDir: "/data/qddir", Role: greenplum.PrimaryRole},
		{DbID: 6, ContentID: -1, Hostname: "standby", DataDir: "/data/standby", Role: greenplum.MirrorRole},
		{DbID: 2, ContentID: 0, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: greenplum.PrimaryRole},
		{DbID: 3, ContentID: 0, Hostname: "msdw1", DataDir: "/data/dbfast_mirror1/seg1", Role: greenplum.MirrorRole},
		{DbID: 4, ContentID: 1, Hostname: "sdw2", DataDir: "/data/dbfast2/seg2", Role: greenplum.PrimaryRole},
		{DbID: 5, ContentID: 1, Hostname: "msdw2", DataDir: "/data/dbfast_mirror2/seg2", Role: greenplum.MirrorRole},
	})

	t.Run("deletes tablespace directories on the master", func(t *testing.T) {
		masterTablespaces := greenplum.SegmentTablespaces{
			16386: {
				Location:    "/tmp/testfs/master/demoDataDir-1/16386",
				UserDefined: 1,
			},
			16387: {
				Location:    "/tmp/testfs/master/demoDataDir-1/16387",
				UserDefined: 1,
			},
			1663: {
				Location:    "/data/qddir/demoDataDir-1",
				UserDefined: 0,
			},
		}

		hub.DeleteTablespaceDirectoriesFunc = func(streams step.OutStreams, dirs []string) error {
			expected := []string{
				"/tmp/testfs/master/demoDataDir-1/16386/1/GPDB_6_301908232",
				"/tmp/testfs/master/demoDataDir-1/16387/1/GPDB_6_301908232",
			}
			sort.Strings(dirs)
			if !reflect.DeepEqual(dirs, expected) {
				t.Errorf("got %v want %v", dirs, expected)
			}
			return nil
		}

		err := hub.DeleteTablespaceDirectoriesOnMaster(&testutils.DevNullWithClose{}, masterTablespaces, "GPDB_6_301908232")
		if err != nil {
			t.Errorf("DeleteTablespaceDirectoriesOnMaster returned error %+v", err)
		}
	})

	t.Run("deletes tablespace directories on the primaries", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		tablespaces := map[int]greenplum.SegmentTablespaces{
			1: {
				16386: {
					Location:    "/tmp/testfs/master/demoDataDir-1/16386",
					UserDefined: 1,
				},
				16387: {
					Location:    "/tmp/testfs/master/demoDataDir-1/16387",
					UserDefined: 1,
				},
				1663: {
					Location:    "/data/qddir/demoDataDir-1",
					UserDefined: 0,
				},
			},
			2: {
				16386: {
					Location:    "/tmp/testfs/primary1/dbfast1/16386",
					UserDefined: 1,
				},
				16387: {
					Location:    "/tmp/testfs/primary1/dbfast1/16387",
					UserDefined: 1,
				},
				1663: {
					Location:    "/data/dbfast1/seg1",
					UserDefined: 0,
				},
			},
			4: {
				16386: {
					Location:    "/tmp/testfs/primary2/dbfast2/16386",
					UserDefined: 1,
				},
				16387: {
					Location:    "/tmp/testfs/primary2/dbfast2/16387",
					UserDefined: 1,
				},
				1663: {
					Location:    "/data/dbfast2/seg2",
					UserDefined: 0,
				},
			},
		}

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().DeleteTablespaceDirectories(
			gomock.Any(),
			equivalentRequest(&idl.DeleteTablespaceRequest{
				Dirs: []string{
					"/tmp/testfs/primary1/dbfast1/16386/2/GPDB_6_301908232",
					"/tmp/testfs/primary1/dbfast1/16387/2/GPDB_6_301908232",
				}}),
		).Return(&idl.DeleteTablespaceReply{}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().DeleteTablespaceDirectories(
			gomock.Any(),
			equivalentRequest(&idl.DeleteTablespaceRequest{
				Dirs: []string{
					"/tmp/testfs/primary2/dbfast2/16386/4/GPDB_6_301908232",
					"/tmp/testfs/primary2/dbfast2/16387/4/GPDB_6_301908232",
				}}),
		).Return(&idl.DeleteTablespaceReply{}, nil)

		master := mock_idl.NewMockAgentClient(ctrl)
		standby := mock_idl.NewMockAgentClient(ctrl)

		agentConns := []*hub.Connection{
			{nil, sdw1, "sdw1", nil},
			{nil, sdw2, "sdw2", nil},
			{nil, master, "master", nil},
			{nil, standby, "standby", nil},
		}

		err := hub.DeleteTablespaceDirectoriesOnPrimaries(agentConns, target, tablespaces, "GPDB_6_301908232")
		if err != nil {
			t.Errorf("DeleteTablespaceDirectoriesOnPrimaries returned error %+v", err)
		}
	})

	t.Run("errors when failing to delete tablespace directories on the primaries", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().DeleteTablespaceDirectories(
			gomock.Any(),
			gomock.Any(),
		).Return(&idl.DeleteTablespaceReply{}, nil)

		expected := errors.New("permission denied")
		failedClient := mock_idl.NewMockAgentClient(ctrl)
		failedClient.EXPECT().DeleteTablespaceDirectories(
			gomock.Any(),
			gomock.Any(),
		).Return(nil, expected)

		agentConns := []*hub.Connection{
			{nil, sdw1, "sdw1", nil},
			{nil, failedClient, "sdw2", nil},
		}

		err := hub.DeleteTablespaceDirectoriesOnPrimaries(agentConns, target, nil, "")

		var multiErr *multierror.Error
		if !xerrors.As(err, &multiErr) {
			t.Fatalf("got error %#v, want type %T", err, multiErr)
		}

		if len(multiErr.Errors) != 1 {
			t.Errorf("received %d errors, want %d", len(multiErr.Errors), 1)
		}

		for _, err := range multiErr.Errors {
			if !xerrors.Is(err, expected) {
				t.Errorf("got error %#v, want %#v", expected, err)
			}
		}
	})
}

// equivalentRequest is a Matcher that can handle differences in order between
// two instances of DeleteTablespaceRequest.Dirs
func equivalentRequest(req *idl.DeleteTablespaceRequest) gomock.Matcher {
	return reqMatcher{req}
}

type reqMatcher struct {
	expected *idl.DeleteTablespaceRequest
}

func (r reqMatcher) Matches(x interface{}) bool {
	actual, ok := x.(*idl.DeleteTablespaceRequest)
	if !ok {
		return false
	}

	// The key here is that Datadirs can be in any order. Sort them before
	// comparison.
	sort.Strings(r.expected.Dirs)
	sort.Strings(actual.Dirs)

	return reflect.DeepEqual(r.expected, actual)
}

func (r reqMatcher) String() string {
	return fmt.Sprintf("is equivalent to %v", r.expected)
}
