package hub

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/greenplum-db/gpupgrade/db"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/greenplum-db/gpupgrade/utils/cluster"
)

func (s *Server) Initialize(in *idl.InitializeRequest, stream idl.CliToHub_InitializeServer) (err error) {
	store, err := s.NewSubstepStateStore("initalize")
	if err != nil {
		return err
	}

	st, err := BeginStep(s.StateDir, "initialize", stream, store)
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

	st.Run(idl.Substep_CONFIG, func(stream step.OutStreams) error {
		return s.fillClusterConfigsSubStep(stream, in)
	})

	st.Run(idl.Substep_START_AGENTS, func(_ step.OutStreams) error {
		_, err := RestartAgents(context.Background(), nil, s.Source.GetHostnames(), s.AgentPort, s.StateDir)
		return err
	})

	return st.Err()
}

func (s *Server) InitializeCreateCluster(in *idl.InitializeCreateClusterRequest, stream idl.CliToHub_InitializeCreateClusterServer) (err error) {
	store, err := s.NewSubstepStateStore("initalize")
	if err != nil {
		return nil
	}

	st, err := BeginStep(s.StateDir, "initialize", stream, store)
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
		return s.GenerateInitsystemConfig(s.TargetPorts)
	})

	st.Run(idl.Substep_INIT_TARGET_CLUSTER, func(stream step.OutStreams) error {
		return s.CreateTargetCluster(stream, s.TargetPorts[0])
	})

	st.Run(idl.Substep_SHUTDOWN_TARGET_CLUSTER, func(stream step.OutStreams) error {
		return s.ShutdownCluster(stream, false)
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

// create old/new clusters, write to disk and re-read from disk to make sure it is "durable"
func (s *Server) fillClusterConfigsSubStep(_ step.OutStreams, request *idl.InitializeRequest) error {
	conn := db.NewDBConn("localhost", int(request.SourcePort), "template1")
	defer conn.Close()

	var err error
	s.Source, err = utils.ClusterFromDB(conn, request.SourceBinDir)
	if err != nil {
		return errors.Wrap(err, "could not retrieve source configuration")
	}

	s.Target = &utils.Cluster{Cluster: new(cluster.Cluster), BinDir: request.TargetBinDir}
	s.UseLinkMode = request.UseLinkMode

	s.TargetPorts, err = assignPorts(s.Source, request.Ports)
	if err != nil {
		return err
	}

	if err := s.SaveConfig(); err != nil {
		return err
	}

	return nil
}

func assignPorts(source *utils.Cluster, ports []uint32) ([]int, error) {
	var intPorts []int
	for _, p := range ports {
		intPorts = append(intPorts, int(p))
	}

	if len(intPorts) == 0 {
		intPorts = defaultTargetPorts(source)
	}

	return sanitize(intPorts), nil
}

// sanitize sorts and deduplicates a slice of port numbers.
func sanitize(ports []int) []int {
	sort.Slice(ports, func(i, j int) bool { return ports[i] < ports[j] })

	dedupe := ports[:0] // point at the same backing array

	var last int
	for i, port := range ports {
		if i == 0 || port != last {
			dedupe = append(dedupe, port)
		}
		last = port
	}

	return dedupe
}

// defaultPorts generates the minimum temporary port range necessary to handle a
// cluster of the given topology. The first port in the list is meant to be used
// for the master.
func defaultTargetPorts(source *utils.Cluster) []int {
	// Partition segments by host in order to correctly assign ports.
	segmentsByHost := make(map[string][]cluster.SegConfig)

	for content, segment := range source.Primaries {
		// Exclude the master for now. We want to give it its own reserved port,
		// which does not overlap with the other segments, so we'll add it back
		// later.
		if content == -1 {
			continue
		}
		segmentsByHost[segment.Hostname] = append(segmentsByHost[segment.Hostname], segment)
	}

	// Start with the pg_upgrade default of 50432. Reserve enough ports to
	// handle the host with the most segments.
	var maxSegs int
	for _, segments := range segmentsByHost {
		if len(segments) > maxSegs {
			maxSegs = len(segments)
		}
	}

	var ports []int
	// Add 1 for the reserved master port
	for i := 0; i < maxSegs+1; i++ {
		ports = append(ports, 50432+i)
	}

	return ports
}

func getAgentPath() (string, error) {
	hubPath, err := os.Executable()
	if err != nil {
		return "", err
	}

	return filepath.Join(filepath.Dir(hubPath), "gpupgrade"), nil
}
