package hub

import (
	"database/sql"
	"fmt"
	"os/exec"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/greenplum-db/gpupgrade/idl"

	"github.com/greenplum-db/gpupgrade/step"

	"github.com/greenplum-db/gpupgrade/utils"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"
)

// ReconfigurePorts executes the tricky sequence of operations required to
// change the ports on a cluster.
//
// TODO: this method needs test coverage.
func ReconfigurePorts(source *utils.Cluster, target *utils.Cluster, stream step.OutStreams) (err error) {
	// 1). bring down the cluster
	err = StopCluster(stream, target)
	if err != nil {
		return xerrors.Errorf("%s failed to stop cluster: %w",
			idl.Substep_RECONFIGURE_PORTS, err)
	}

	// 2). bring up the master(fts will not "freak out", etc)
	err = StartMasterInUtilityMode(target)

	if err != nil {
		return xerrors.Errorf("%s failed to start target cluster in utility mode: %w",
			idl.Substep_RECONFIGURE_PORTS, err)
	}

	// 3). rewrite gp_segment_configuration with the updated port number
	err = updateSegmentConfiguration(source, target)
	if err != nil {
		return err
	}

	// 4). bring down the master
	err = StopMasterInUtilityMode(target)

	if err != nil {
		return xerrors.Errorf("%s failed to stop target cluster in utility mode: %w",
			idl.Substep_RECONFIGURE_PORTS, err)
	}

	// 5). rewrite the "port" field in the master's postgresql.conf
	err = RewritePortsInMasterConfiguration(target, source.MasterPort())
	if err != nil {
		return xerrors.Errorf("%s failed to execute sed command: %w",
			idl.Substep_RECONFIGURE_PORTS, err)
	}

	// 6. bring up the cluster
	err = StartCluster(stream, target)
	if err != nil {
		return xerrors.Errorf("%s failed to start target cluster: %w",
			idl.Substep_RECONFIGURE_PORTS, err)
	}

	return nil
}

func RewritePortsInMasterConfiguration(cluster *utils.Cluster, newPortNumber int) error {
	return rewritePortsInMasterConfigurationFunc(cluster, newPortNumber)
}

var rewritePortsInMasterConfigurationFunc = rewritePortsInMasterConfiguration

func rewritePortsInMasterConfiguration(cluster *utils.Cluster, newPortNumber int) error {
	script := fmt.Sprintf(
		"sed 's/port=%d/port=%d/' %[3]s/postgresql.conf > %[3]s/postgresql.conf.updated && "+
			"mv %[3]s/postgresql.conf %[3]s/postgresql.conf.bak && "+
			"mv %[3]s/postgresql.conf.updated %[3]s/postgresql.conf",
		cluster.MasterPort(), newPortNumber, cluster.MasterDataDir(),
	)
	gplog.Debug("executing command: %+v", script) // TODO: Move this debug log into ExecuteLocalCommand()
	cmd := exec.Command("bash", "-c", script)
	_, err := cmd.Output()
	return err
}

func updateSegmentConfiguration(source, target *utils.Cluster) error {
	connURI := fmt.Sprintf("postgresql://localhost:%d/template1?gp_session_role=utility&allow_system_table_mods=true&search_path=", target.MasterPort())
	targetDB, err := sql.Open("pgx", connURI)
	defer func() {
		closeErr := targetDB.Close()
		if closeErr != nil {
			closeErr = xerrors.Errorf("closing connection to new master db: %w", closeErr)
			err = multierror.Append(err, closeErr)
		}
	}()
	if err != nil {
		return xerrors.Errorf("%s failed to open connection to utility master: %w",
			idl.Substep_RECONFIGURE_PORTS, err)
	}
	err = ClonePortsFromCluster(targetDB, source.Cluster)
	if err != nil {
		return xerrors.Errorf("%s failed to clone ports: %w",
			idl.Substep_RECONFIGURE_PORTS, err)
	}
	return nil
}
