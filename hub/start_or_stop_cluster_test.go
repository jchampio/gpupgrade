package hub

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
	"github.com/greenplum-db/gpupgrade/utils"

	. "github.com/onsi/gomega"
)

func IsPostmasterRunningCmd_Errors() {
	os.Stderr.WriteString("exit status 2")
	os.Exit(2)
}

func init() {
	exectest.RegisterMains(
		IsPostmasterRunningCmd_Errors,
	)
}

func TestStartOrStopCluster(t *testing.T) {
	g := NewGomegaWithT(t)

	var source *utils.Cluster
	cluster := cluster.NewCluster([]cluster.SegConfig{{
		ContentID: -1,
		DbID:      1,
		Port:      15432,
		Hostname:  "localhost",
		DataDir:   "basedir/seg-1",
	}})
	source = &utils.Cluster{
		Cluster: cluster,
		BinDir:  "/source/bindir",
		Version: dbconn.GPDBVersion{},
	}
	utils.System.RemoveAll = func(s string) error { return nil }
	utils.System.MkdirAll = func(s string, perm os.FileMode) error { return nil }

	startStopClusterCmd = nil
	isPostmasterRunningCmd = nil

	defer func() {
		startStopClusterCmd = exec.Command
		isPostmasterRunningCmd = exec.Command
	}()

	t.Run("isPostmasterRunning succeeds", func(t *testing.T) {
		isPostmasterRunningCmd = exectest.NewCommandWithVerifier(Success,
			func(path string, args ...string) {
				g.Expect(path).To(Equal("bash"))
				g.Expect(args).To(Equal([]string{"-c", "pgrep -F basedir/seg-1/postmaster.pid"}))
			})

		err := IsPostmasterRunning(DevNull, source)
		g.Expect(err).ToNot(HaveOccurred())
	})

	t.Run("isPostmasterRunning fails", func(t *testing.T) {
		isPostmasterRunningCmd = exectest.NewCommand(IsPostmasterRunningCmd_Errors)

		err := IsPostmasterRunning(DevNull, source)
		g.Expect(err).To(HaveOccurred())
	})

	t.Run("stopCluster", func(t *testing.T) {
		setup := func(t *testing.T) {
			// XXX The cost-to-benefit ratio of this verification is very high.
			// It pins a lot of behavior that we don't care about.

			verify := func(t *testing.T, path string, args []string, expected string) {
				t.Helper()

				if path != "bash" {
					t.Errorf("executable was %q, want %q", path, "bash")
				}

				if len(args) != 2 {
					t.Errorf("expected two arguments to %q, got %q", path, args)
					return
				}

				if args[0] != "-c" {
					t.Errorf("first argument to %q must be -c; got %q", path, args[0])
				}

				expected = fmt.Sprintf("source /source/bindir/../greenplum_path.sh && %s", expected)
				if args[1] != expected {
					t.Errorf("bash command was %q, want %q", args[1], expected)
				}
			}

			pgCtlCmd = exectest.NewCommandWithVerifier(Success,
				func(path string, args ...string) {
					expected := "/source/bindir/pg_ctl stop -m fast -w -D basedir/seg-1"
					verify(t, path, args, expected)
				})

			gpstartCmd = exectest.NewCommandWithVerifier(Success,
				func(path string, args ...string) {
					expected := "/source/bindir/gpstart -a -d basedir/seg-1"
					verify(t, path, args, expected)
				})

			gpstopCmd = exectest.NewCommandWithVerifier(Success,
				func(path string, args ...string) {
					expected := "/source/bindir/gpstop -a -f -d basedir/seg-1"
					verify(t, path, args, expected)
				})
		}

		teardown := func() {
			pgCtlCmd = nil
			gpstartCmd = nil
			gpstopCmd = nil
		}

		t.Run("runs expected command sequence", func(t *testing.T) {
			setup(t)
			defer teardown()

			if err := StopCluster(DevNull, source); err != nil {
				t.Errorf("StopCluster() returned error %+v", err)
			}
		})

		t.Run("does not fail if pg_ctl stop fails", func(t *testing.T) {
			setup(t)
			defer teardown()

			pgCtlCmd = exectest.NewCommand(Failure)
			if err := StopCluster(DevNull, source); err != nil {
				t.Errorf("StopCluster() returned error %+v", err)
			}
		})

		t.Run("fails if gpstart fails", func(t *testing.T) {
			setup(t)
			defer teardown()

			gpstartCmd = exectest.NewCommand(Failure)
			err := StopCluster(DevNull, source)

			var actual *exec.ExitError
			if !xerrors.As(err, &actual) {
				t.Fatalf("StopCluster() returned %#v, want type %T", err, actual)
			}
		})

		t.Run("fails if gpstop fails", func(t *testing.T) {
			setup(t)
			defer teardown()

			gpstopCmd = exectest.NewCommand(Failure)
			err := StopCluster(DevNull, source)

			var actual *exec.ExitError
			if !xerrors.As(err, &actual) {
				t.Fatalf("StopCluster() returned %#v, want type %T", err, actual)
			}
		})
	})

	t.Run("startCluster successfully starts up cluster", func(t *testing.T) {
		startStopClusterCmd = exectest.NewCommandWithVerifier(Success,
			func(path string, args ...string) {
				g.Expect(path).To(Equal("bash"))
				g.Expect(args).To(Equal([]string{"-c", "source /source/bindir/../greenplum_path.sh " +
					"&& /source/bindir/gpstart -a -d basedir/seg-1"}))
			})

		err := StartCluster(DevNull, source)
		g.Expect(err).ToNot(HaveOccurred())
	})
}
