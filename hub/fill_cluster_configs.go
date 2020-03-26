package hub

import (
	"database/sql"
	"os"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"

	"github.com/greenplum-db/gpupgrade/db"
	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/upgrade"
)

// create source/target clusters, write to disk and re-read from disk to make sure it is "durable"
func FillClusterConfigsSubStep(config *Config, conn *sql.DB, _ step.OutStreams, request *idl.InitializeRequest, saveConfig func() error) error {
	// Assign a new universal upgrade identifier.
	config.UpgradeID = upgrade.NewID()

	if err := CheckSourceClusterConfiguration(conn); err != nil {
		return err
	}

	// XXX ugly; we should just use the conn we're passed, but our DbConn
	// concept (which isn't really used) gets in the way
	dbconn := db.NewDBConn("localhost", int(request.SourcePort), "template1")
	source, err := greenplum.ClusterFromDB(dbconn, request.SourceBinDir)
	if err != nil {
		return errors.Wrap(err, "could not retrieve source configuration")
	}

	config.Source = source
	config.Target = &greenplum.Cluster{BinDir: request.TargetBinDir}
	config.UseLinkMode = request.UseLinkMode

	var ports []int
	for _, p := range request.Ports {
		ports = append(ports, int(p))
	}

	config.TargetInitializeConfig, err = AssignDatadirsAndPorts(config.Source, ports, config.UpgradeID)
	if err != nil {
		return err
	}

	if err := saveConfig(); err != nil {
		return err
	}

	return nil
}

func AssignDatadirsAndPorts(source *greenplum.Cluster, ports []int, upgradeID upgrade.ID) (InitializeConfig, error) {
	if len(ports) == 0 {
		port := 50432
		numberOfSegments := len(source.Mirrors) + len(source.Primaries) + 2 // +2 for master/standby
		if (numberOfSegments + port) > 65535 {
			numberOfSegments = 65535 - port
		}

		for i := 0; i < numberOfSegments; i++ {
			ports = append(ports, port)
			port++
		}
	} else {
		ports = sanitize(ports)
	}

	return assignDatadirsAndCustomPorts(source, ports, upgradeID)
}

// can return an error if we run out of ports to use
func assignDatadirsAndCustomPorts(source *greenplum.Cluster, ports []int, upgradeID upgrade.ID) (InitializeConfig, error) {
	targetInitializeConfig := InitializeConfig{}

	var segPrefix string
	nextPortIndex := 0

	// XXX we can't handle a masterless cluster elsewhere in the code; we may
	// want to remove the "ok" check here and force NewCluster to error out
	if master, ok := source.Primaries[-1]; ok {
		// Reserve a port for the master.
		if nextPortIndex > len(ports)-1 {
			return InitializeConfig{}, errors.New("not enough ports")
		}

		// Save the segment prefix for later.
		var err error
		segPrefix, err = GetMasterSegPrefix(master.DataDir)
		if err != nil {
			return InitializeConfig{}, err
		}

		master.Port = ports[nextPortIndex]
		master.DataDir = upgrade.TempDataDir(master.DataDir, segPrefix, upgradeID)
		targetInitializeConfig.Master = master
		nextPortIndex++
	}

	if standby, ok := source.Mirrors[-1]; ok {
		// Reserve a port for the standby.
		if nextPortIndex > len(ports)-1 {
			return InitializeConfig{}, errors.New("not enough ports")
		}
		standby.Port = ports[nextPortIndex]
		standby.DataDir = upgrade.TempDataDir(standby.DataDir, segPrefix, upgradeID)
		targetInitializeConfig.Standby = standby
		nextPortIndex++
	}

	portIndexByHost := make(map[string]int)

	for _, content := range source.ContentIDs {
		// Skip the master segment
		if content == -1 {
			continue
		}

		segment := source.Primaries[content]

		if portIndex, ok := portIndexByHost[segment.Hostname]; ok {
			if portIndex > len(ports)-1 {
				return InitializeConfig{}, errors.New("not enough ports")
			}
			segment.Port = ports[portIndex]
			portIndexByHost[segment.Hostname]++
		} else {
			if nextPortIndex > len(ports)-1 {
				return InitializeConfig{}, errors.New("not enough ports")
			}
			segment.Port = ports[nextPortIndex]
			portIndexByHost[segment.Hostname] = nextPortIndex + 1
		}
		segment.DataDir = upgrade.TempDataDir(segment.DataDir, segPrefix, upgradeID)

		targetInitializeConfig.Primaries = append(targetInitializeConfig.Primaries, segment)
	}

	for _, content := range source.ContentIDs {
		// Skip the standby segment
		if content == -1 {
			continue
		}

		if segment, ok := source.Mirrors[content]; ok {
			if portIndex, ok := portIndexByHost[segment.Hostname]; ok {
				if portIndex > len(ports)-1 {
					return InitializeConfig{}, errors.New("not enough ports")
				}
				segment.Port = ports[portIndex]
				portIndexByHost[segment.Hostname]++
			} else {
				if nextPortIndex > len(ports)-1 {
					return InitializeConfig{}, errors.New("not enough ports")
				}
				segment.Port = ports[nextPortIndex]
				portIndexByHost[segment.Hostname] = nextPortIndex + 1
			}
			segment.DataDir = upgrade.TempDataDir(segment.DataDir, segPrefix, upgradeID)

			targetInitializeConfig.Mirrors = append(targetInitializeConfig.Mirrors, segment)
		}
	}

	return targetInitializeConfig, nil
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

func getAgentPath() (string, error) {
	hubPath, err := os.Executable()
	if err != nil {
		return "", err
	}

	return filepath.Join(filepath.Dir(hubPath), "gpupgrade"), nil
}
