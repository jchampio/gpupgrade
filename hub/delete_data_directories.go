// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"fmt"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/net/context"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/upgrade"
)

var DeleteTablespaceDirectoriesFunc = upgrade.DeleteTablespaceDirectories

func DeleteMirrorAndStandbyDataDirectories(agentConns []*Connection, cluster *greenplum.Cluster) error {
	return deleteDataDirectories(agentConns, cluster, false)
}

func DeletePrimaryDataDirectories(agentConns []*Connection, cluster *greenplum.Cluster) error {
	return deleteDataDirectories(agentConns, cluster, true)
}

func deleteDataDirectories(agentConns []*Connection, cluster *greenplum.Cluster, primaries bool) error {
	request := func(conn *Connection) error {
		segs := cluster.SelectSegments(func(seg *greenplum.SegConfig) bool {
			if seg.Hostname != conn.Hostname {
				return false
			}

			if primaries {
				return seg.IsPrimary()
			}
			return seg.Role == greenplum.MirrorRole
		})

		if len(segs) == 0 {
			// This can happen if there are no segments matching the filter on a host
			return nil
		}

		req := new(idl.DeleteDataDirectoriesRequest)
		for _, seg := range segs {
			datadir := seg.DataDir
			req.Datadirs = append(req.Datadirs, datadir)
		}

		_, err := conn.AgentClient.DeleteDataDirectories(context.Background(), req)
		return err
	}

	return ExecuteRPC(agentConns, request)
}

func DeleteTablespaceDirectories(streams step.OutStreams, agentConns []*Connection, target *greenplum.Cluster, tablespaces greenplum.Tablespaces) error {
	identifier := fmt.Sprintf("GPDB_%d_%s", target.Version.SemVer.Major, target.CatalogVersion)

	var wg sync.WaitGroup
	errs := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- DeleteTablespaceDirectoriesOnMaster(streams, tablespaces.GetMasterTablespaces(), identifier)
	}()

	errs <- DeleteTablespaceDirectoriesOnPrimaries(agentConns, target, tablespaces, identifier)

	wg.Wait()
	close(errs)

	var mErr *multierror.Error
	for err := range errs {
		mErr = multierror.Append(mErr, err)
	}

	return mErr.ErrorOrNil()
}

func DeleteTablespaceDirectoriesOnMaster(streams step.OutStreams, masterTablespaces greenplum.SegmentTablespaces, identifier string) error {
	var dirs []string
	for _, tsInfo := range masterTablespaces {
		if !tsInfo.IsUserDefined() {
			continue
		}

		path := filepath.Join(tsInfo.Location, strconv.Itoa(greenplum.MasterDbid), identifier)
		dirs = append(dirs, path)
	}

	return DeleteTablespaceDirectoriesFunc(streams, dirs)
}

func DeleteTablespaceDirectoriesOnPrimaries(agentConns []*Connection, target *greenplum.Cluster, tablespaces greenplum.Tablespaces, identifier string) error {
	request := func(conn *Connection) error {
		primaries := target.SelectSegments(func(seg *greenplum.SegConfig) bool {
			return seg.IsOnHost(conn.Hostname) && seg.IsPrimary() && !seg.IsMaster()
		})

		if len(primaries) == 0 {
			return nil
		}

		var dirs []string
		for _, seg := range primaries {
			segTablespaces := tablespaces[seg.DbID]
			for _, tsInfo := range segTablespaces {
				if !tsInfo.IsUserDefined() {
					continue
				}

				path := filepath.Join(tsInfo.Location, strconv.Itoa(seg.DbID), identifier)
				dirs = append(dirs, path)
			}
		}

		req := &idl.DeleteTablespaceRequest{Dirs: dirs}
		_, err := conn.AgentClient.DeleteTablespaceDirectories(context.Background(), req)
		return err
	}

	return ExecuteRPC(agentConns, request)
}
