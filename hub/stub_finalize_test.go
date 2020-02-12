package hub

import (
	"database/sql"

	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/utils/cluster"

	"github.com/greenplum-db/gpupgrade/utils"
)

func StubReconfigurePortsToSucceed() {
	StubStopClusterCommand(func() error {
		return nil
	})

	StubStartClusterCommand(func() error {
		return nil
	})

	StubStartMasterInUtilityMode(func() error {
		return nil
	})

	StubClonePortsFromCluster(func() error {
		return nil
	})

	StubStopMasterInUtilityMode(func() error {
		return nil
	})

	StubRewritePortsInMasterConfigurationFunc(func() error {
		return nil
	})
}

func StubClonePortsFromCluster(newClonePortsFromClusterFunc func() error) {
	clonePortsFromClusterFunc = func(db *sql.DB, src *cluster.Cluster) error {
		return newClonePortsFromClusterFunc()
	}
}

func StubRewritePortsInMasterConfigurationFunc(newRewritePortsInMasterConfigurationFunc func() error) {
	rewritePortsInMasterConfigurationFunc = func(cluster *utils.Cluster, newPortNumber int) error {
		return newRewritePortsInMasterConfigurationFunc()
	}
}

func StubUpgradeStandby(newFunc UpgradeStandbyFunc) {
	upgradeStandbyFunc = newFunc
}

func ResetUpgradeStandby() {
	upgradeStandbyFunc = upgradeStandby
}

func StubStopClusterCommand(newStopClusterCommand func() error) {
	stopClusterFunc = func(stream step.OutStreams, cluster *utils.Cluster) error {
		return newStopClusterCommand()
	}
}

func StubStartClusterCommand(newStartClusterCommand func() error) {
	startClusterFunc = func(stream step.OutStreams, cluster *utils.Cluster) error {
		return newStartClusterCommand()
	}
}

func StubStartMasterInUtilityMode(newStartMasterInUtilityMode func() error) {
	startMasterInUtilityModeFunc = func(cluster *utils.Cluster) error {
		return newStartMasterInUtilityMode()
	}
}

func StubStopMasterInUtilityMode(newStopMasterInUtilityMode func() error) {
	stopMasterInUtilityModeFunc = func(cluster *utils.Cluster) error {
		return newStopMasterInUtilityMode()
	}
}
