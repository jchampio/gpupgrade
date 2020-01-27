package hub

import (
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/blang/semver"
	"github.com/pkg/errors"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
	"github.com/greenplum-db/gpupgrade/utils"
)

// These variables provide injection points for exectest.Command.
// XXX we badly need a way to combine these; they're proliferating
var (
	isPostmasterRunningCmd = exec.Command
	startStopClusterCmd    = exec.Command
	pgCtlCmd               = exec.Command
	gpstopCmd              = exec.Command
	gpstartCmd             = exec.Command
)

func (h *Hub) ShutdownCluster(stream OutStreams, isSource bool) error {
	if isSource {
		err := StopCluster(stream, h.Source)
		if err != nil {
			return errors.Wrap(err, "failed to stop source cluster")
		}
	} else {
		err := StopCluster(stream, h.Target)
		if err != nil {
			return errors.Wrap(err, "failed to stop target cluster")
		}
	}

	return nil
}

// runGPCommand returns the results of an exec.Cmd run for the given GPDB
// utility invocation.
//
// XXX There are way too many inputs to this.
func runGPCommand(cmdFunc exectest.Command, path, args string, stream OutStreams, cluster *utils.Cluster) error {
	envScript := filepath.Join(cluster.BinDir, "..", "greenplum_path.sh")
	path = filepath.Join(cluster.BinDir, path)

	cmd := cmdFunc("bash", "-c", fmt.Sprintf("source %s && %s %s",
		envScript,
		path,
		args,
	))

	cmd.Stdout = stream.Stdout()
	cmd.Stderr = stream.Stderr()

	// Don't leak the hub environment to the subprocesses.
	cmd.Env = []string{}

	// XXX If this is a 5X cluster, gpstart and gpstop need the
	// MASTER_DATA_DIRECTORY environment variable to be set due to a bug in the
	// -d option's implementation.
	if cluster.Version.SemVer.LT(semver.MustParse("6.0.0")) {
		mdd := fmt.Sprintf("MASTER_DATA_DIRECTORY=%s", cluster.MasterDataDir())
		cmd.Env = append(cmd.Env, mdd)
	}

	fmt.Fprintf(stream.Stdout(), "executing %q %q\n", cmd.Path, cmd.Args)
	return cmd.Run()
}

func StopCluster(stream OutStreams, cluster *utils.Cluster) error {
	// pg_ctl stop the master
	err := runGPCommand(pgCtlCmd,
		"pg_ctl", fmt.Sprintf("stop -m fast -w -D %s", cluster.MasterDataDir()),
		stream, cluster)
	if err != nil {
		// Because `pg_ctl stop` can fail because the master is already stopped,
		// but it does not give us a programmatic distinction between a hard
		// failure and an already-stopped failure, we explicitly ignore non-zero
		// exit codes here and rely on the following gpstart to catch any
		// problems.
		fmt.Fprintf(stream.Stderr(), "ignoring pg_ctl failure (is master already stopped?): %+v\n", err)
		fmt.Fprintf(stream.Stderr(), "if gpstart fails, check the above error\n")
	}

	// gpstart the cluster to fully bring up the cluster
	err = runGPCommand(gpstartCmd,
		"gpstart", fmt.Sprintf("-a -d %s", cluster.MasterDataDir()),
		stream, cluster)
	if err != nil {
		return err
	}

	// gpstop the cluster...the cluster is now properly shut down
	return runGPCommand(gpstopCmd,
		"gpstop", fmt.Sprintf("-a -f -d %s", cluster.MasterDataDir()),
		stream, cluster)
}

func StartCluster(stream OutStreams, cluster *utils.Cluster) error {
	return startStopCluster(stream, cluster, false)
}

func startStopCluster(stream OutStreams, cluster *utils.Cluster, stop bool) error {
	// TODO: why can't we call IsPostmasterRunning for the !stop case?  If we do, we get this on the pipeline:
	// Usage: pgrep [-flvx] [-d DELIM] [-n|-o] [-P PPIDLIST] [-g PGRPLIST] [-s SIDLIST]
	// [-u EUIDLIST] [-U UIDLIST] [-G GIDLIST] [-t TERMLIST] [PATTERN]
	//  pgrep: pidfile not valid
	// TODO: should we actually return an error if we try to gpstop an already stopped cluster?
	cmdName := "gpstart"
	if stop {
		cmdName = "gpstop"
		err := IsPostmasterRunning(stream, cluster)
		if err != nil {
			return err
		}
	}

	cmd := startStopClusterCmd("bash", "-c",
		fmt.Sprintf("source %[1]s/../greenplum_path.sh && %[1]s/%[2]s -a -d %[3]s",
			cluster.BinDir,
			cmdName,
			cluster.MasterDataDir(),
		))

	cmd.Stdout = stream.Stdout()
	cmd.Stderr = stream.Stderr()

	return cmd.Run()
}

func IsPostmasterRunning(stream OutStreams, cluster *utils.Cluster) error {
	cmd := isPostmasterRunningCmd("bash", "-c",
		fmt.Sprintf("pgrep -F %s/postmaster.pid",
			cluster.MasterDataDir(),
		))

	cmd.Stdout = stream.Stdout()
	cmd.Stderr = stream.Stderr()

	return cmd.Run()
}
