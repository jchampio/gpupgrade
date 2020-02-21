package hub_test

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"github.com/greenplum-db/gpupgrade/hub"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/idl/mock_idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

type renameSpy struct {
	calls []*renameCall
}

type renameCall struct {
	originalName string
	newName      string
}

func (s *renameSpy) TimesCalled() int {
	return len(s.calls)
}

func (s *renameSpy) Call(i int) *renameCall {
	return s.calls[i-1]
}

func TestSwapDataDirectories(t *testing.T) {
	testhelper.SetupTestLogger() // init gplog

	afterEach := func() {
		utils.System = utils.InitializeSystemFunctions()
	}

	t.Run("it renames data directories for source and target master data dirs", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		spy := &renameSpy{}

		utils.System.Rename = spy.renameFunc()

		source := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: -1, DataDir: "/some/data/directory", Role: utils.PrimaryRole},
			{ContentID: 100, DataDir: "/some/data/directory/primary1", Role: utils.PrimaryRole},
		})

		target := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: -1, DataDir: "/some/qddir_upgrade/dataDirectory", Role: utils.PrimaryRole},
			{ContentID: 100, DataDir: "/some/segment1_upgrade/dataDirectory", Role: utils.PrimaryRole},
		})

		config := &hub.Config{
			Source: source,
			Target: target,
		}

		client := mock_idl.NewMockAgentClient(ctrl)
		client.EXPECT().ReconfigureDataDirectories(gomock.Any(), gomock.Any())

		hub.SwapDataDirectories(hub.MakeHub(config, map[string]idl.AgentClient{
			"": client,
		}))

		if spy.TimesCalled() != 2 {
			t.Errorf("got Rename called %v times, wanted %v times",
				spy.TimesCalled(),
				2)
		}

		spy.assertDirectoriesMoved(t,
			"/some/data/directory",
			"/some/data/directory_old")

		spy.assertDirectoriesMoved(t,
			"/some/qddir_upgrade/dataDirectory",
			"/some/data/directory")

		if source.Primaries[-1].DataDir != "/some/data/directory" {
			t.Errorf("got %v, wanted it to be unchanged as %v",
				source.Primaries[-1].DataDir,
				"/some/data/directory")
		}

		if target.Primaries[-1].DataDir != "/some/qddir_upgrade/dataDirectory" {
			t.Errorf("got %v, wanted it to be unchanged as %v",
				target.Primaries[-1].DataDir,
				"/some/qddir_upgrade/dataDirectory")
		}
	})

	t.Run("it returns an error if the directories cannot be renamed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		defer afterEach()

		utils.System.Rename = func(oldpath, newpath string) error {
			return errors.New("failure to rename")
		}

		source := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: 99, DataDir: "/some/data/directory", Role: utils.PrimaryRole},
		})

		target := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: 99, DataDir: "/some/data/directory", Role: utils.PrimaryRole},
		})

		config := &hub.Config{
			Source: source,
			Target: target,
		}

		client := mock_idl.NewMockAgentClient(ctrl)
		client.EXPECT().ReconfigureDataDirectories(gomock.Any(), gomock.Any())

		err := hub.SwapDataDirectories(hub.MakeHub(config, map[string]idl.AgentClient{
			"": client,
		}))

		if err == nil {
			t.Fatalf("got nil for an error during SwapDataDirectories, wanted a failure to move directories: %+v", err)
		}
	})

	t.Run("it does not modify the cluster state if there is an error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		defer afterEach()

		utils.System.Rename = func(oldpath, newpath string) error {
			return errors.New("failure to rename")
		}

		source := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: 99, DataDir: "/some/data/directory", Role: utils.PrimaryRole},
		})

		target := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: 99, DataDir: "/some/data/directory_upgrade", Role: utils.PrimaryRole},
		})

		config := &hub.Config{
			Source: source,
			Target: target,
		}

		client := mock_idl.NewMockAgentClient(ctrl)
		client.EXPECT().ReconfigureDataDirectories(gomock.Any(), gomock.Any())

		err := hub.SwapDataDirectories(hub.MakeHub(config, map[string]idl.AgentClient{
			"": client,
		}))

		if err == nil {
			t.Fatalf("got nil for an error during SwapDataDirectories, wanted a failure to move directories: %+v", err)
		}

		assertDataDir_NOT_Modified(t,
			config.Source.Primaries[99].DataDir,
			"/some/data/directory",
		)

		assertDataDir_NOT_Modified(t,
			config.Target.Primaries[99].DataDir,
			"/some/data/directory_upgrade",
		)
	})

	t.Run("it tells each agent to reconfigure data directories for the segments", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		spy := &renameSpy{}
		utils.System.Rename = spy.renameFunc()

		source := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: 99, Hostname: "host1", DataDir: "/some/data/directory/99", Role: utils.PrimaryRole},
			{ContentID: 100, Hostname: "host2", DataDir: "/some/data/directory/100", Role: utils.PrimaryRole},
		})

		target := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: 99, Hostname: "host1", DataDir: "/some/data/directory_upgrade/99", Role: utils.PrimaryRole},
			{ContentID: 100, Hostname: "host2", DataDir: "/some/data/directory_upgrade/100", Role: utils.PrimaryRole},
		})

		config := &hub.Config{
			Source: source,
			Target: target,
		}

		client1 := mock_idl.NewMockAgentClient(ctrl)
		client1.EXPECT().ReconfigureDataDirectories(gomock.Any(),
			&idl.ReconfigureDataDirRequest{
				Pairs: []*idl.RenamePair{
					{
						Src: "/some/data/directory/99",
						Dst: "/some/data/directory/99_old",
					},
					{
						Src: "/some/data/directory_upgrade/99",
						Dst: "/some/data/directory/99",
					},
				},
			},
		)

		client2 := mock_idl.NewMockAgentClient(ctrl)
		client2.EXPECT().ReconfigureDataDirectories(gomock.Any(),
			&idl.ReconfigureDataDirRequest{
				Pairs: []*idl.RenamePair{
					{
						Src: "/some/data/directory/100",
						Dst: "/some/data/directory/100_old",
					},
					{
						Src: "/some/data/directory_upgrade/100",
						Dst: "/some/data/directory/100",
					},
				},
			},
		)

		h := hub.MakeHub(config, map[string]idl.AgentClient{
			"host1": client1,
			"host2": client2,
		})

		err := hub.SwapDataDirectories(h)
		if err != nil {
			t.Errorf("expected no error, got %#v", err)
		}
	})

	t.Run("it errors out if the call to the agents fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		spy := &renameSpy{}
		utils.System.Rename = spy.renameFunc()

		source := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: 99, Hostname: "host1", DataDir: "/some/data/directory/99", Role: utils.PrimaryRole},
			{ContentID: 100, Hostname: "host2", DataDir: "/some/data/directory/100", Role: utils.PrimaryRole},
		})

		target := hub.MustCreateCluster(t, []utils.SegConfig{
			{ContentID: 99, Hostname: "host1", DataDir: "/some/data/directory_upgrade/99", Role: utils.PrimaryRole},
			{ContentID: 100, Hostname: "host2", DataDir: "/some/data/directory_upgrade/100", Role: utils.PrimaryRole},
		})

		config := &hub.Config{
			Source: source,
			Target: target,
		}

		client1 := mock_idl.NewMockAgentClient(ctrl)
		client1.EXPECT().ReconfigureDataDirectories(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("error"))

		client2 := mock_idl.NewMockAgentClient(ctrl)
		client2.EXPECT().ReconfigureDataDirectories(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("error"))

		h := hub.MakeHub(config, map[string]idl.AgentClient{
			"host1": client1,
			"host2": client2,
		})

		err := hub.SwapDataDirectories(h)

		if err == nil {
			t.Errorf("got no errors from agents, expected an error for each host")
		}
	})

}

func assertDataDirModified(t *testing.T, newDataDir, expectedDataDir string) {
	if newDataDir != expectedDataDir {
		t.Errorf("got new data dir of %v, wanted %v",
			newDataDir, expectedDataDir)
	}
}

func assertDataDir_NOT_Modified(t *testing.T, newDataDir, expectedDataDir string) {
	if newDataDir != expectedDataDir {
		t.Errorf("got new data dir of %v, wanted %v",
			newDataDir, expectedDataDir)
	}
}

func (spy *renameSpy) assertDirectoriesMoved(t *testing.T, originalName string, newName string) {
	var call *renameCall

	for _, c := range spy.calls {
		if c.originalName == originalName && c.newName == newName {
			call = c
			break
		}
	}

	if call == nil {
		t.Errorf("got no calls to rename %v to %v, expected 1 call", originalName, newName)
	}
}

func (spy *renameSpy) renameFunc() func(oldpath string, newpath string) error {
	return func(originalName, newName string) error {
		spy.calls = append(spy.calls, &renameCall{
			originalName: originalName,
			newName:      newName,
		})

		return nil
	}
}
