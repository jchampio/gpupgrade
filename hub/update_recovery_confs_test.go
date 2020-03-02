package hub_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/hub"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/idl/mock_idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

func TestUpdateRecoveryConfs(t *testing.T) {
	ctx := context.Background()

	t.Run("it makes an UpdateRecoveryConfs request to each agent", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// using distinct ports to make this easier to test
		soource := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: -1, DbID: 1, Port: 4040, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Port: 4041, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: "m"},
			{ContentID: 0, DbID: 3, Port: 4042, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: "p"},
			{ContentID: 1, DbID: 4, Port: 4043, Hostname: "sdw2", DataDir: "/data/dbfast2/seg1", Role: "p"},
			{ContentID: 2, DbID: 5, Port: 4044, Hostname: "sdw2", DataDir: "/data/dbfast3/seg2", Role: "p"},
			{ContentID: 0, DbID: 6, Port: 4045, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: "m"},
			{ContentID: 1, DbID: 7, Port: 4046, Hostname: "sdw1", DataDir: "/data/dbfast_mirror2/seg1", Role: "m"},
			{ContentID: 2, DbID: 8, Port: 4047, Hostname: "sdw1", DataDir: "/data/dbfast_mirror3/seg2", Role: "m"},
		})

		target := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: -1, DbID: 1, Port: 6778, Hostname: "mdw", DataDir: "/data/qddir_upgrade/seg-1", Role: "p"},
			{ContentID: 0, DbID: 3, Port: 6780, Hostname: "sdw1", DataDir: "/data/dbfast1_upgrade/seg0", Role: "p"},
			{ContentID: 1, DbID: 4, Port: 6781, Hostname: "sdw2", DataDir: "/data/dbfast2_upgrade/seg1", Role: "p"},
			{ContentID: 2, DbID: 5, Port: 6782, Hostname: "sdw2", DataDir: "/data/dbfast3_upgrade/seg2", Role: "p"},
		})

		config := hub.InitializeConfig{
			Mirrors: []utils.SegConfig{
				{ContentID: 0, DbID: 6, Port: 6783, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1_upgrade/seg0", Role: "m"},
				{ContentID: 1, DbID: 7, Port: 6784, Hostname: "sdw1", DataDir: "/data/dbfast_mirror2_upgrade/seg1", Role: "m"},
				{ContentID: 2, DbID: 8, Port: 6785, Hostname: "sdw1", DataDir: "/data/dbfast_mirror3_upgrade/seg2", Role: "m"},
			},
		}

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().
			UpdateRecoveryConfs(
				gomock.Any(),
				&idl.UpdateRecoveryConfsRequest{RecoveryConfInfos: []*idl.RecoveryConfInfo{
					{TargetPrimaryPort: 6781, SourcePrimaryPort: 4043, TargetMirrorDataDir: "/data/dbfast_mirror2_upgrade/seg1"},
					{TargetPrimaryPort: 6782, SourcePrimaryPort: 4044, TargetMirrorDataDir: "/data/dbfast_mirror3_upgrade/seg2"},
				}}).
			Return(&idl.UpdateRecoveryConfsReply{}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().
			UpdateRecoveryConfs(
				gomock.Any(),
				&idl.UpdateRecoveryConfsRequest{RecoveryConfInfos: []*idl.RecoveryConfInfo{
					{TargetPrimaryPort: 6780, SourcePrimaryPort: 4042, TargetMirrorDataDir: "/data/dbfast_mirror1_upgrade/seg0"},
				}}).
			Return(&idl.UpdateRecoveryConfsReply{}, nil)

		agents := []*hub.Connection{
			{Hostname: "sdw1", AgentClient: sdw1},
			{Hostname: "sdw2", AgentClient: sdw2},
		}

		err := hub.UpdateRecoveryConfs(ctx, agents, soource, target, config)
		if err != nil {
			t.Errorf("unexpected error: %+v", err)
		}
	})

	t.Run("when there is an error after making a recovery request it gets returned", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// using distinct ports to make this easier to test
		source := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: -1, DbID: 1, Port: 4040, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Port: 4041, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: "m"},
			{ContentID: 0, DbID: 3, Port: 4042, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: "p"},
			{ContentID: 0, DbID: 6, Port: 4045, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: "m"},
		})

		target := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: -1, DbID: 1, Port: 6778, Hostname: "mdw", DataDir: "/data/qddir_upgrade/seg-1", Role: "p"},
			{ContentID: 0, DbID: 3, Port: 6780, Hostname: "sdw1", DataDir: "/data/dbfast1_upgrade/seg0", Role: "p"},
		})

		config := hub.InitializeConfig{
			Mirrors: []utils.SegConfig{
				{ContentID: 0, DbID: 6, Port: 6783, Hostname: "sdw1", DataDir: "/data/dbfast_mirror1_upgrade/seg0", Role: "m"},
			},
		}

		expected := errors.New("An Err")

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().
			UpdateRecoveryConfs(gomock.Any(), gomock.Any()).
			Return(&idl.UpdateRecoveryConfsReply{}, expected)

		agents := []*hub.Connection{
			{Hostname: "sdw1", AgentClient: sdw1},
		}

		err := hub.UpdateRecoveryConfs(ctx, agents, source, target, config)

		var multiErr *multierror.Error
		if !xerrors.As(err, &multiErr) {
			t.Fatalf("got error %#v, want type %T", err, multiErr)
		}

		if len(multiErr.Errors) != 1 {
			t.Errorf("received %d errors, want %d", len(multiErr.Errors), 1)
		}

		for _, err := range multiErr.Errors {
			if !xerrors.Is(err, expected) {
				t.Errorf("wanted error %+v got %+v", expected, err)
			}
		}
	})

	t.Run("when there are no mirrors to update, it does not make a request", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().
			UpdateRecoveryConfs(gomock.Any(), gomock.Any()).Times(0)

		agents := []*hub.Connection{
			{Hostname: "sdw1", AgentClient: sdw1},
		}

		err := hub.UpdateRecoveryConfs(ctx, agents, &utils.Cluster{}, &utils.Cluster{}, hub.InitializeConfig{})
		if err != nil {
			t.Errorf("got unexpected error: %+v", err)
		}
	})

	t.Run("when there are no mirrors to update on a connection, it does not make a request", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		// using distinct ports to make this easier to test
		source := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: -1, DbID: 1, Port: 4040, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Port: 4041, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: "m"},
			{ContentID: 0, DbID: 3, Port: 4042, Hostname: "sdw1", DataDir: "/data/dbfast1/seg0", Role: "p"},
			{ContentID: 0, DbID: 6, Port: 4045, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg0", Role: "m"},
		})

		target := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: -1, DbID: 1, Port: 6778, Hostname: "mdw", DataDir: "/data/qddir_upgrade/seg-1", Role: "p"},
			{ContentID: 0, DbID: 3, Port: 6780, Hostname: "sdw1", DataDir: "/data/dbfast1_upgrade/seg0", Role: "p"},
		})

		config := hub.InitializeConfig{
			Mirrors: []utils.SegConfig{
				{ContentID: 0, DbID: 6, Port: 6783, Hostname: "sdw1", DataDir: "/data/dbfast_mirror1_upgrade/seg0", Role: "m"},
			},
		}

		sdw1 := mock_idl.NewMockAgentClient(ctrl)
		sdw1.EXPECT().
			UpdateRecoveryConfs(
				gomock.Any(),
				&idl.UpdateRecoveryConfsRequest{RecoveryConfInfos: []*idl.RecoveryConfInfo{
					{TargetPrimaryPort: 6780, SourcePrimaryPort: 4042, TargetMirrorDataDir: "/data/dbfast_mirror1_upgrade/seg0"},
				}}).
			Return(&idl.UpdateRecoveryConfsReply{}, nil)

		sdw2 := mock_idl.NewMockAgentClient(ctrl)
		sdw2.EXPECT().
			UpdateRecoveryConfs(
				gomock.Any(),
				gomock.Any()).
			Times(0)

		agents := []*hub.Connection{
			{Hostname: "sdw1", AgentClient: sdw1},
			{Hostname: "sdw2", AgentClient: sdw2},
		}

		err := hub.UpdateRecoveryConfs(ctx, agents, source, target, config)
		if err != nil {
			t.Errorf("got unexpected error: %+v", err)
		}
	})
}
