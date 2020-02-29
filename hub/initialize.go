package hub

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
)

const connectionString = "postgresql://localhost:%d/template1?gp_session_role=utility&search_path="

func (s *Server) Initialize(in *idl.InitializeRequest, stream idl.CliToHub_InitializeServer) (err error) {
	st, err := step.Begin(s.StateDir, "initialize", stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := st.Finish(); ferr != nil {
			err = multierror.Append(err, ferr).ErrorOrNil()
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("initialize: %s", err))
		}
	}()

	conn, err := sql.Open("pgx", fmt.Sprintf(connectionString, in.SourcePort))
	if err != nil {
		return err
	}
	defer conn.Close() // XXX error?

	st.Run(idl.Substep_CONFIG, func(stream step.OutStreams) error {
		return FillClusterConfigsSubStep(s.Config, conn, stream, in, s.SaveConfig)
	})

	st.Run(idl.Substep_START_AGENTS, func(_ step.OutStreams) error {
		_, err := RestartAgents(context.Background(), nil, s.Source.GetHostnames(), s.AgentPort, s.StateDir)
		return err
	})

	return st.Err()
}

func (s *Server) InitializeCreateCluster(in *idl.InitializeCreateClusterRequest, stream idl.CliToHub_InitializeCreateClusterServer) (err error) {
	st, err := step.Begin(s.StateDir, "initialize", stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := st.Finish(); ferr != nil {
			err = multierror.Append(err, ferr).ErrorOrNil()
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("initialize: %s", err))
		}
	}()

	st.Run(idl.Substep_CREATE_TARGET_CONFIG, func(_ step.OutStreams) error {
		return s.GenerateInitsystemConfig()
	})

	st.Run(idl.Substep_INIT_TARGET_CLUSTER, func(stream step.OutStreams) error {
		return s.CreateTargetCluster(stream)
	})

	st.Run(idl.Substep_SHUTDOWN_TARGET_CLUSTER, func(stream step.OutStreams) error {
		return StopCluster(stream, s.Target, false)
	})

	st.Run(idl.Substep_BACKUP_TARGET_MASTER, func(stream step.OutStreams) error {
		sourceDir := s.Target.MasterDataDir()
		targetDir := filepath.Join(s.StateDir, originalMasterBackupName)
		return RsyncMasterDataDir(stream, sourceDir, targetDir)
	})

	st.AlwaysRun(idl.Substep_CHECK_UPGRADE, func(stream step.OutStreams) error {
		return s.CheckUpgrade(stream)
	})

	return st.Err()
}
