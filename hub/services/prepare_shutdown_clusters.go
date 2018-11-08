package services

import (
	"fmt"

	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	pb "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	multierror "github.com/hashicorp/go-multierror"
)

func (h *Hub) PrepareShutdownClusters(in *pb.PrepareShutdownClustersRequest, stream pb.CliToHub_PrepareShutdownClustersServer) error {
	gplog.Info("starting PrepareShutdownClusters()")

	return h.ShutdownClusters(stream)
}

func (h *Hub) ShutdownClusters(stream pb.CliToHub_PrepareShutdownClustersServer) error {
	var shutdownErr error

	// This type links each utils.Cluster to its protobuf type code.
	type cluster struct {
		*utils.Cluster
		typeCode pb.WhichCluster
	}

	clusters := []cluster{
		{h.source, pb.WhichCluster_SOURCE},
		{h.target, pb.WhichCluster_TARGET},
	}

	step := h.checklist.GetStepWriter(upgradestatus.SHUTDOWN_CLUSTERS)

	step.ResetStateDir()
	step.MarkInProgress()

	for _, cluster := range clusters {
		err := StopCluster(cluster.Cluster)
		if err != nil {
			gplog.Error(err.Error())
			shutdownErr = multierror.Append(shutdownErr, err)
		}

		reply := &pb.PrepareShutdownClustersReply{
			Cluster:   cluster.typeCode,
			Succeeded: err == nil,
		}

		err = stream.Send(reply)
		if err != nil {
			gplog.Error(err.Error())
		}
	}

	if shutdownErr != nil {
		step.MarkFailed()
		return shutdownErr
	}

	step.MarkComplete()
	return nil
}

func StopCluster(c *utils.Cluster) error {
	if !IsPostmasterRunning(c) {
		return nil
	}

	masterDataDir := c.MasterDataDir()
	gpstopShellArgs := fmt.Sprintf("source %[1]s/../greenplum_path.sh; %[1]s/gpstop -a -d %[2]s", c.BinDir, masterDataDir)

	gplog.Info("gpstop args: %+v", gpstopShellArgs)
	_, err := c.ExecuteLocalCommand(gpstopShellArgs)
	if err != nil {
		return err
	}

	return nil
}

func IsPostmasterRunning(c *utils.Cluster) bool {
	masterDataDir := c.MasterDataDir()
	checkPidCmd := fmt.Sprintf("pgrep -F %s/postmaster.pid", masterDataDir)

	_, err := c.ExecuteLocalCommand(checkPidCmd)
	if err != nil {
		gplog.Error("Could not determine whether the cluster with MASTER_DATA_DIRECTORY: %s is running: %+v",
			masterDataDir, err)
		return false
	}

	return true
}
