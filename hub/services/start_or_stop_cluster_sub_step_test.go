package services_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"

	"github.com/greenplum-db/gpupgrade/idl/mock_idl"
	"github.com/greenplum-db/gpupgrade/testutils/exectest"
	"github.com/greenplum-db/gpupgrade/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/greenplum-db/gpupgrade/hub/services"
)

func TestStartOrStopCluster(t *testing.T) {
	g := NewGomegaWithT(t)
	ctrl := gomock.NewController(GinkgoT())
	defer ctrl.Finish()

	mockStream := mock_idl.NewMockCliToHub_ExecuteServer(ctrl)
	mockStream.EXPECT().
		Send(gomock.Any()).
		AnyTimes()

	var buf bytes.Buffer
	var source *utils.Cluster
	cluster := cluster.NewCluster([]cluster.SegConfig{cluster.SegConfig{ContentID: -1, DbID: 1, Port: 15432, Hostname: "localhost", DataDir: "basedir/seg-1"}})
	source = &utils.Cluster{
		Cluster:    cluster,
		BinDir:     "/source/bindir",
		ConfigPath: "my/config/path",
		Version:    dbconn.GPDBVersion{},
	}
	utils.System.RemoveAll = func(s string) error { return nil }
	utils.System.MkdirAll = func(s string, perm os.FileMode) error { return nil }

	t.Run("isPostmasterRunning succeeds", func(t *testing.T) {
		SetExecCommand(exectest.NewCommandWithVerifier(exectest.Success,
			func(path string, args ...string) {
				g.Expect(path).To(Equal("bash"))
				g.Expect(args).To(Equal([]string{"-c", "pgrep -F basedir/seg-1/postmaster.pid"}))
			}))
		defer ResetExecCommand()

		err := IsPostmasterRunning(mockStream, &buf, source)
		g.Expect(err).ToNot(HaveOccurred())
	})

	t.Run("isPostmasterRunning fails", func(t *testing.T) {
		SetExecCommand(exectest.NewCommand(exectest.Failure))
		defer ResetExecCommand()

		err := IsPostmasterRunning(mockStream, &buf, source)
		g.Expect(err).To(HaveOccurred())
	})

	t.Run("stopCluster successfully shuts down cluster", func(t *testing.T) {
		pgrep := exectest.NewCommandWithVerifier(exectest.Success,
			func(path string, args ...string) {
				g.Expect(path).To(Equal("bash"))
				g.Expect(args).To(Equal([]string{"-c", "pgrep -F basedir/seg-1/postmaster.pid"}))
			})

		gpstop := exectest.NewCommandWithVerifier(exectest.Success,
			func(path string, args ...string) {
				g.Expect(path).To(Equal("bash"))
				g.Expect(args).To(Equal([]string{"-c", "source /source/bindir/../greenplum_path.sh " +
					"&& /source/bindir/gpstop -a -d basedir/seg-1"}))
			})

		SetExecCommand(exectest.Select(func(path string, args ...string) exectest.Command {
			if len(args) < 2 {
				goto unexpected
			}

			switch {
			case strings.Contains(args[1], "pgrep"):
				return pgrep
			case strings.Contains(args[1], "gpstop"):
				return gpstop
			}

		unexpected:
			t.Fatalf("unexpected command: path %q, args %q", path, args)
			panic("unreachable")
		}))
		defer ResetExecCommand()

		err := StopCluster(mockStream, &buf, source)
		g.Expect(err).ToNot(HaveOccurred())
	})

	t.Run("stopCluster detects that cluster is already shutdown", func(t *testing.T) {
		// Since the pgrep fails, we expect no call to gpstop. If one is made,
		// we rely on the verifier here to catch it.
		SetExecCommand(exectest.NewCommandWithVerifier(exectest.Failure,
			func(path string, args ...string) {
				g.Expect(path).To(Equal("bash"))
				g.Expect(args).To(Equal([]string{"-c", "pgrep -F basedir/seg-1/postmaster.pid"}))
			}))
		defer ResetExecCommand()

		err := StopCluster(mockStream, &buf, source)
		g.Expect(err).To(HaveOccurred())
	})

	t.Run("startCluster successfully starts up cluster", func(t *testing.T) {
		SetExecCommand(exectest.NewCommandWithVerifier(exectest.Success,
			func(path string, args ...string) {
				g.Expect(path).To(Equal("bash"))
				g.Expect(args).To(Equal([]string{"-c", "source /source/bindir/../greenplum_path.sh " +
					"&& /source/bindir/gpstart -a -d basedir/seg-1"}))
			}))
		defer ResetExecCommand()

		err := StartCluster(mockStream, &buf, source)
		g.Expect(err).ToNot(HaveOccurred())
	})
}
