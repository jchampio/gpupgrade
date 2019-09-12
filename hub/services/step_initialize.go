package services

import (
	"fmt"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpupgrade/db"
	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func (h *Hub) TellHubToInitializeUpgrade(ctx context.Context, in *idl.TellHubToInitializeUpgradeRequest) (*idl.TellHubToInitializeUpgradeReply, error) {
	gplog.Info("starting the hub....")

	err := h.CheckConfigStep()
	if err != nil {
		return &idl.TellHubToInitializeUpgradeReply{}, err
	}

	err = h.PrepareStartAgentsStep()

	return &idl.TellHubToInitializeUpgradeReply{}, err
}

func (h *Hub) CheckConfigStep() error {
	gplog.Info("starting %s", upgradestatus.CONFIG)

	step, err := h.InitializeStep(upgradestatus.CONFIG)
	if err != nil {
		gplog.Error(err.Error())
		return err
	}

	dbConn := db.NewDBConn("localhost", 0, "template1")
	defer dbConn.Close()
	err = ReloadAndCommitClusterStep(h.source, dbConn)
	if err != nil {
		gplog.Error(err.Error())
		step.MarkFailed()
		return err
	}

	step.MarkComplete()
	return err
}

// ReloadAndCommitCluster() will fill in a utils.Cluster using a database
// connection and additionally write the results to disk.
func ReloadAndCommitClusterStep(cluster *utils.Cluster, conn *dbconn.DBConn) error {
	newCluster, err := utils.ClusterFromDB(conn, cluster.BinDir, cluster.ConfigPath)
	if err != nil {
		return errors.Wrap(err, "could not retrieve cluster configuration")
	}

	*cluster = *newCluster
	err = cluster.Commit()
	if err != nil {
		return errors.Wrap(err, "could not save cluster configuration")
	}

	return nil
}

func (h *Hub) PrepareStartAgentsStep() error {
	gplog.Info("starting %s", upgradestatus.START_AGENTS)

	step, err := h.InitializeStep(upgradestatus.START_AGENTS)
	if err != nil {
		gplog.Error(err.Error())
		return err
	}

	err = StartAgentsStep(h.source, h.target)
	if err != nil {
		gplog.Error(err.Error())
		step.MarkFailed()
	} else {
		step.MarkComplete()
	}

	return err
}

func StartAgentsStep(source *utils.Cluster, target *utils.Cluster) error {
	logStr := "start agents on master and hosts"
	agentPath := filepath.Join(target.BinDir, "gpupgrade_agent")
	runAgentCmd := func(contentID int) string { return agentPath + " --daemonize" }

	errStr := "Failed to start all gpupgrade_agents"

	remoteOutput, err := source.ExecuteOnAllHosts(logStr, runAgentCmd)
	if err != nil {
		return errors.Wrap(err, errStr)
	}

	errMessage := func(contentID int) string {
		return fmt.Sprintf("Could not start gpupgrade_agent on segment with contentID %d", contentID)
	}
	source.CheckClusterError(remoteOutput, errStr, errMessage, true)

	// Log successful starts. Agents print their port and PID to stdout.
	for content, output := range remoteOutput.Stdouts {
		// XXX If there are failures, does it matter what agents have
		// successfully started, or do we just want to stop all of them and kick
		// back to the user?
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
