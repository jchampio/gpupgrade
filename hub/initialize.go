package hub

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/db"
	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

func (h *Hub) Initialize(in *idl.InitializeRequest, stream idl.CliToHub_InitializeServer) (err error) {
	log, err := utils.System.OpenFile(
		filepath.Join(utils.GetStateDir(), "initialize.log"),
		os.O_WRONLY|os.O_CREATE,
		0600,
	)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := log.Close(); closeErr != nil {
			err = multierror.Append(err,
				xerrors.Errorf("failed to close initialize log: %w", closeErr))
		}
	}()

	initializeStream := newMultiplexedStream(stream, log)

	_, err = log.WriteString("\nInitialize in progress.\n")
	if err != nil {
		return xerrors.Errorf("failed writing to initialize log: %w", err)
	}

	err = h.Substep(initializeStream, upgradestatus.CONFIG,
		func(stream OutStreams) error {
			return h.fillClusterConfigsSubStep(stream, in.OldBinDir, in.NewBinDir, int(in.OldPort))
		})
	if err != nil {
		return err
	}

	err = h.Substep(initializeStream, upgradestatus.START_AGENTS, h.startAgentsSubStep)
	if err != nil {
		return err
	}

	return nil
}

func (h *Hub) InitializeCreateCluster(in *idl.InitializeCreateClusterRequest, stream idl.CliToHub_InitializeCreateClusterServer) (err error) {
	log, err := utils.System.OpenFile(
		filepath.Join(utils.GetStateDir(), "initialize.log"),
		os.O_WRONLY|os.O_APPEND,
		0600,
	)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := log.Close(); closeErr != nil {
			err = multierror.Append(err,
				xerrors.Errorf("failed to close initialize log: %w", closeErr))
		}
	}()

	initializeStream := newMultiplexedStream(stream, log)

	_, err = log.WriteString("\nInitialize hub in progress.\n")
	if err != nil {
		return xerrors.Errorf("failed writing to initialize log for hub: %w", err)
	}

	var targetMasterPort int
	err = h.Substep(initializeStream, upgradestatus.CREATE_TARGET_CONFIG,
		func(_ OutStreams) error {
			var err error
			targetMasterPort, err = h.GenerateInitsystemConfig(in.Ports)
			return err
		})
	if err != nil {
		return err
	}

	err = h.Substep(initializeStream, upgradestatus.SHUTDOWN_SOURCE_CLUSTER,
		func(stream OutStreams) error {
			return StopCluster(stream, h.source)
		})
	if err != nil {
		return err
	}

	err = h.Substep(initializeStream, upgradestatus.INIT_TARGET_CLUSTER,
		func(stream OutStreams) error {
			return h.CreateTargetCluster(stream, targetMasterPort)
		})
	if err != nil {
		return err
	}

	err = h.Substep(initializeStream, upgradestatus.SHUTDOWN_TARGET_CLUSTER,
		func(stream OutStreams) error {
			return h.ShutdownCluster(stream, false)
		})
	if err != nil {
		return err
	}

	return h.Substep(initializeStream, upgradestatus.CHECK_UPGRADE, h.CheckUpgrade)
}

// create old/new clusters, write to disk and re-read from disk to make sure it is "durable"
func (h *Hub) fillClusterConfigsSubStep(_ OutStreams, oldBinDir, newBinDir string, oldPort int) error {
	conn := db.NewDBConn("localhost", oldPort, "template1")
	defer conn.Close()

	var err error
	h.Source, err = utils.ClusterFromDB(conn, oldBinDir)
	if err != nil {
		return errors.Wrap(err, "could not retrieve source configuration")
	}

	h.Target = &utils.Cluster{Cluster: new(cluster.Cluster), BinDir: newBinDir}

	if err := h.SaveConfig(); err != nil {
		return err
	}

	// link in source/target to hub
	// TODO: remove once we deduplicate
	h.source = h.Source
	h.target = h.Target

	return nil
}

func getAgentPath() (string, error) {
	hubPath, err := os.Executable()
	if err != nil {
		return "", err
	}

	return filepath.Join(filepath.Dir(hubPath), "gpupgrade"), nil
}

// TODO: use the implementation in RestartAgents() for this function and combine them
func (h *Hub) startAgentsSubStep(stream OutStreams) error {
	source := h.source
	stateDir := h.conf.StateDir

	// XXX If there are failures, does it matter what agents have successfully
	// started, or do we just want to stop all of them and kick back to the
	// user?
	logStr := "start agents on master and hosts"

	agentPath, err := getAgentPath()
	if err != nil {
		return errors.Errorf("failed to get the hub executable path %v", err)
	}

	// XXX State directory handling on agents needs to be improved. See issue
	// #127: all agents will silently recreate that directory if it doesn't
	// already exist. Plus, ExecuteOnAllHosts() doesn't let us control whether
	// we execute locally or via SSH for the master, so we don't know whether
	// GPUPGRADE_HOME is going to be inherited.
	runAgentCmd := func(contentID int) string {
		return agentPath + " agent --daemonize --state-directory " + stateDir
	}

	errStr := "Failed to start all gpupgrade agents"

	remoteOutput, err := source.ExecuteOnAllHosts(logStr, runAgentCmd)
	if err != nil {
		return errors.Wrap(err, errStr)
	}

	errMessage := func(contentID int) string {
		return fmt.Sprintf("Could not start gpupgrade agent on segment with contentID %d", contentID)
	}
	source.CheckClusterError(remoteOutput, errStr, errMessage, true)

	// Agents print their port and PID to stdout; log them for posterity.
	for content, output := range remoteOutput.Stdouts {
		if remoteOutput.Errors[content] == nil {
			gplog.Info("[%s] %s", source.Segments[content].Hostname, output)
		}
	}

	if remoteOutput.NumErrors > 0 {
		// CheckClusterError() will have already logged each error.
		return errors.New("could not start agents on segment hosts; see log for details")
	}

	return nil
}
