// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/upgrade"
	"github.com/greenplum-db/gpupgrade/utils"
)

var ErrMissingMirrorsAndStandby = errors.New("Source cluster does not have mirrors and/or standby. Cannot restore source cluster. Please contact support.")

func (s *Server) Revert(_ *idl.RevertRequest, stream idl.CliToHub_RevertServer) (err error) {
	st, err := step.Begin(s.StateDir, idl.Step_REVERT, stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := st.Finish(); ferr != nil {
			err = multierror.Append(err, ferr).ErrorOrNil()
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("revert: %s", err))
		}
	}()

	if !s.Source.HasAllMirrorsAndStandby() {
		return errors.New("Source cluster does not have mirrors and/or standby. Cannot restore source cluster. Please contact support.")
	}

	// ensure that agentConns is populated
	_, err = s.AgentConns()
	if err != nil {
		return xerrors.Errorf("connect to gpupgrade agent: %w", err)
	}

	// If the target cluster is started, it must be stopped.
	if s.Target != nil {
		st.AlwaysRun(idl.Substep_SHUTDOWN_TARGET_CLUSTER, func(streams step.OutStreams) error {
			running, err := s.Target.IsMasterRunning(streams)
			if err != nil {
				return err
			}

			if !running {
				return nil
			}

			if err := s.Target.Stop(streams); err != nil {
				return xerrors.Errorf("stopping target cluster: %w", err)
			}

			return nil
		})
	}

	// Restoring the source master and primaries is only needed if upgrading the
	// primaries had started.
	// TODO: For now we use if the source master is not running to determine this.
	running, err := s.Source.IsMasterRunning(st.Streams())
	if err != nil {
		return err
	}

	if !running && s.UseLinkMode {
		hasRun, err := step.HasRun(idl.Step_EXECUTE, idl.Substep_START_TARGET_CLUSTER)
		if err != nil {
			return err
		}

		st.Run(idl.Substep_RESTORE_SOURCE_CLUSTER, func(stream step.OutStreams) error {
			if hasRun {
				if err := RsyncMasterAndPrimaries(stream, s.agentConns, s.Source); err != nil {
					return err
				}

				return RsyncMasterAndPrimariesTablespaces(stream, s.agentConns, s.Source, s.Tablespaces)
			}

			// since the target cluster was not started, just restore pg_control.old
			// to pg_control
			return RestoreMasterAndPrimariesPgControl(s.agentConns, s.Source)
		})
	}

	if s.TargetInitializeConfig.Primaries != nil {
		st.Run(idl.Substep_DELETE_PRIMARY_DATADIRS, func(_ step.OutStreams) error {
			return DeletePrimaryDataDirectories(s.agentConns, s.TargetInitializeConfig.Primaries)
		})
	}

	if s.TargetInitializeConfig.Master.DataDir != "" {
		st.Run(idl.Substep_DELETE_MASTER_DATADIR, func(streams step.OutStreams) error {
			datadir := s.TargetInitializeConfig.Master.DataDir
			return upgrade.DeleteDirectories([]string{datadir}, upgrade.PostgresFiles, streams)
		})

		st.Run(idl.Substep_DELETE_TABLESPACES, func(streams step.OutStreams) error {
			return DeleteTargetTablespaces(streams, s.agentConns, s.Config.Target, s.TargetCatalogVersion, s.Tablespaces)
		})
	}

	var archiveDir string
	st.Run(idl.Substep_ARCHIVE_LOG_DIRECTORIES, func(_ step.OutStreams) error {
		// Archive log directory on master
		oldDir, err := utils.GetLogDir()
		if err != nil {
			return err
		}
		archiveDir = filepath.Join(filepath.Dir(oldDir), upgrade.GetArchiveDirectoryName(s.UpgradeID, time.Now()))

		gplog.Debug("moving directory %q to %q", oldDir, archiveDir)
		if err = utils.Move(oldDir, archiveDir); err != nil {
			return err
		}

		return ArchiveSegmentLogDirectories(s.agentConns, s.Config.Target.MasterHostname(), archiveDir)
	})

	st.Run(idl.Substep_DELETE_SEGMENT_STATEDIRS, func(_ step.OutStreams) error {
		return DeleteStateDirectories(s.agentConns, s.Source.MasterHostname())
	})

	// If the source cluster is not running, it must be started.
	st.AlwaysRun(idl.Substep_START_SOURCE_CLUSTER, func(streams step.OutStreams) error {
		running, err = s.Source.IsMasterRunning(streams)
		if err != nil {
			return err
		}

		if running {
			return nil
		}

		err = s.Source.Start(streams)
		var exitErr *exec.ExitError
		if xerrors.As(err, &exitErr) {
			// In copy mode the gpdb 5x source cluster mirrors do not come
			// up causing gpstart to return a non-zero exit status.
			// This substep fails preventing the following substep steps
			// from running including gprecoverseg.
			// TODO: For 5X investigate how to check for this case and not
			//  ignore all errors with exit code 1.
			if !s.UseLinkMode && exitErr.ExitCode() == 1 {
				return nil
			}
		}

		if err != nil {
			return xerrors.Errorf("starting source cluster: %w", err)
		}

		return nil
	})

	if !s.UseLinkMode {
		st.Run(idl.Substep_RESTORE_SOURCE_CLUSTER, func(streams step.OutStreams) error {
			return Recoverseg(streams, s.Source)
		})
	}

	message := &idl.Message{Contents: &idl.Message_Response{Response: &idl.Response{Data: map[string]string{
		idl.ResponseKey_source_port.String():                  strconv.Itoa(s.Source.MasterPort()),
		idl.ResponseKey_source_master_data_directory.String(): s.Source.MasterDataDir(),
		idl.ResponseKey_source_version.String():               s.Source.Version.VersionString,
		idl.ResponseKey_revert_log_archive_directory.String(): archiveDir,
	}}}}
	if err := stream.Send(message); err != nil {
		return xerrors.Errorf("sending response message: %w", err)
	}

	return st.Err()
}
