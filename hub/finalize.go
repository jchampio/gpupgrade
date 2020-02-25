package hub

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
)

func (s *Server) Finalize(_ *idl.FinalizeRequest, stream idl.CliToHub_FinalizeServer) (err error) {
	st, err := BeginStep(s.StateDir, "finalize", stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := st.Finish(); ferr != nil {
			err = multierror.Append(err, ferr).ErrorOrNil()
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("finalize: %s", err))
		}
	}()

	// This runner runs all commands against the target cluster.
	targetRunner := &greenplumRunner{
		masterPort:          s.Target.MasterPort(),
		masterDataDirectory: s.Target.MasterDataDir(),
		binDir:              s.Target.BinDir,
	}

	if s.Source.HasStandby() {
		st.Run(idl.Substep_FINALIZE_UPGRADE_STANDBY, func(streams step.OutStreams) error {
			// XXX this probably indicates a bad abstraction
			targetRunner.streams = streams

			return UpgradeStandby(targetRunner, StandbyConfig{
				Port:          s.TargetInitializeConfig.Standby.Port,
				Hostname:      s.Source.StandbyHostname(),
				DataDirectory: s.Source.StandbyDataDirectory() + "_upgrade",
			})
		})
	}

	// TODO only do this if there are mirrors!
	st.Run(idl.Substep_FINALIZE_UPGRADE_MIRRORS, func(streams step.OutStreams) error {
		// XXX this probably indicates a bad abstraction
		targetRunner.streams = streams

		return UpgradeMirrors(s.StateDir, &s.TargetInitializeConfig, targetRunner)
	})

	connURI := fmt.Sprintf("postgresql://localhost:%d/template1", s.Target.MasterPort())
	db, err := sql.Open("pgx", connURI)
	tx, err := db.Begin()
	_, _ = tx.Query("SELECT gp_request_fts_probe_scan();")
	_ = tx.Commit()
	time.Sleep(30)

	st.Run(idl.Substep_FINALIZE_SHUTDOWN_TARGET_CLUSTER, func(streams step.OutStreams) error {
		return StopCluster(streams, s.Target, false)
	})

	st.Run(idl.Substep_FINALIZE_START_TARGET_MASTER, func(streams step.OutStreams) error {
		return StartMasterOnly(streams, s.Target, false)
	})

	// Once UpdateCatalogWithPortInformation && UpdateMasterPostgresqlConf is executed, the port on which the target
	// cluster starts is changed in the catalog and postgresql.conf, however the server config.json target port is
	// still the old port on which the target cluster was initialized.
	// TODO: if any steps needs to connect to the new cluster (that should use new port), we should either
	// write it to the config.json or add some way to identify the state.
	st.Run(idl.Substep_FINALIZE_UPDATE_CATALOG_WITH_PORT, func(streams step.OutStreams) error {
		return UpdateCatalogWithPortInformation(s.Source, s.Target)
	})

	st.Run(idl.Substep_FINALIZE_SHUTDOWN_TARGET_MASTER, func(streams step.OutStreams) error {
		return StopMasterOnly(streams, s.Target, false)
	})

	st.Run(idl.Substep_FINALIZE_UPDATE_POSTGRESQL_CONF, func(streams step.OutStreams) error {
		return UpdateMasterPostgresqlConf(s.Source, s.Target)
	})

	st.Run(idl.Substep_FINALIZE_START_TARGET_CLUSTER, func(streams step.OutStreams) error {
		return StartCluster(streams, s.Target, false)
	})

	return st.Err()
}
