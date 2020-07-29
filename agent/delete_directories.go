// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/upgrade"
)

var DeleteDirectoriesFunc = upgrade.DeleteDirectories

func (s *Server) DeleteStateDirectory(ctx context.Context, in *idl.DeleteStateDirectoryRequest) (*idl.DeleteStateDirectoryReply, error) {
	gplog.Info("got a request to delete the state directory from the hub")

	// pass an empty []string to avoid check for any pre-existing files,
	// this call might come in before any stateDir files are created
	err := DeleteDirectoriesFunc([]string{s.conf.StateDir}, []string{}, step.DevNullStream)
	return &idl.DeleteStateDirectoryReply{}, err
}

func (s *Server) DeleteDataDirectories(ctx context.Context, in *idl.DeleteDataDirectoriesRequest) (*idl.DeleteDataDirectoriesReply, error) {
	gplog.Info("got a request to delete data directories from the hub")

	err := DeleteDirectoriesFunc(in.Datadirs, upgrade.PostgresFiles, step.DevNullStream)
	return &idl.DeleteDataDirectoriesReply{}, err
}

func (s *Server) DeleteTablespaceDirectories(ctx context.Context, in *idl.DeleteTablespaceRequest) (*idl.DeleteTablespaceReply, error) {
	gplog.Info("received request to delete tablespace directories from the hub")

	err := upgrade.DeleteTablespaceDirectories(&testutils.DevNullWithClose{}, in.GetDirs())
	return &idl.DeleteTablespaceReply{}, err
}
