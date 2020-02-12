package hub

import (
	"fmt"
	"os/exec"

	"github.com/pkg/errors"

	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/utils"
)

var isPostmasterRunningCmd = exec.Command
var startStopClusterCmd = exec.Command

func (s *Server) ShutdownCluster(stream step.OutStreams, isSource bool) error {
	if isSource {
		err := StopCluster(stream, s.Source)
		if err != nil {
			return errors.Wrap(err, "failed to stop source cluster")
		}
	} else {
		err := StopCluster(stream, s.Target)
		if err != nil {
			return errors.Wrap(err, "failed to stop target cluster")
		}
	}

	return nil
}

var stopClusterFunc = stopCluster
var startClusterFunc = startCluster
var startMasterInUtilityModeFunc = startMasterInUtilityMode
var stopMasterInUtilityModeFunc = stopMasterInUtilityMode

func StopCluster(stream step.OutStreams, cluster *utils.Cluster) error {
	return stopClusterFunc(stream, cluster)
}

func StartCluster(stream step.OutStreams, cluster *utils.Cluster) error {
	return startClusterFunc(stream, cluster)
}

func StartMasterInUtilityMode(cluster *utils.Cluster) error {
	return startMasterInUtilityModeFunc(cluster)
}

func StopMasterInUtilityMode(cluster *utils.Cluster) error {
	return stopMasterInUtilityModeFunc(cluster)
}

func stopMasterInUtilityMode(cluster *utils.Cluster) error {
	script := fmt.Sprintf("source %s/../greenplum_path.sh && %s/gpstop -aim -d %s",
		cluster.BinDir, cluster.BinDir, cluster.MasterDataDir())
	cmd := exec.Command("bash", "-c", script)
	_, err := cmd.Output()
	return err
}

func startMasterInUtilityMode(cluster *utils.Cluster) error {
	script := fmt.Sprintf("source %s/../greenplum_path.sh && %s/gpstart -am -d %s",
		cluster.BinDir, cluster.BinDir, cluster.MasterDataDir())
	cmd := exec.Command("bash", "-c", script)
	_, err := cmd.Output()

	return err
}

func startCluster(stream step.OutStreams, cluster *utils.Cluster) error {
	// TODO: why can't we call IsPostmasterRunning for the !stop case?  If we do, we get this on the pipeline:
	// Usage: pgrep [-flvx] [-d DELIM] [-n|-o] [-P PPIDLIST] [-g PGRPLIST] [-s SIDLIST]
	// [-u EUIDLIST] [-U UIDLIST] [-G GIDLIST] [-t TERMLIST] [PATTERN]
	//  pgrep: pidfile not valid
	// TODO: should we actually return an error if we try to gpstop an already stopped cluster?
	cmdName := "gpstart"

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

func stopCluster(stream step.OutStreams, cluster *utils.Cluster) error {
	// TODO: why can't we call IsPostmasterRunning for the !stop case?  If we do, we get this on the pipeline:
	// Usage: pgrep [-flvx] [-d DELIM] [-n|-o] [-P PPIDLIST] [-g PGRPLIST] [-s SIDLIST]
	// [-u EUIDLIST] [-U UIDLIST] [-G GIDLIST] [-t TERMLIST] [PATTERN]
	//  pgrep: pidfile not valid
	// TODO: should we actually return an error if we try to gpstop an already stopped cluster?
	cmdName := "gpstop"
	err := IsPostmasterRunning(stream, cluster)
	if err != nil {
		return err
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

func IsPostmasterRunning(stream step.OutStreams, cluster *utils.Cluster) error {
	cmd := isPostmasterRunningCmd("bash", "-c",
		fmt.Sprintf("pgrep -F %s/postmaster.pid",
			cluster.MasterDataDir(),
		))

	cmd.Stdout = stream.Stdout()
	cmd.Stderr = stream.Stderr()

	return cmd.Run()
}
