package hub

import (
	"database/sql"
	"sort"

	"github.com/pkg/errors"

	"github.com/greenplum-db/gpupgrade/db"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/utils"
)

// create old/new clusters, write to disk and re-read from disk to make sure it is "durable"
func FillClusterConfigsSubStep(config *Config, conn *sql.DB, _ step.OutStreams, request *idl.InitializeRequest, saveConfig func() error) error {
	if err := CheckSourceClusterConfiguration(conn); err != nil {
		return err
	}

	// XXX ugly; we should just use the conn we're passed, but our DbConn
	// concept (which isn't really used) gets in the way
	dbconn := db.NewDBConn("localhost", int(request.SourcePort), "template1")
	source, err := utils.ClusterFromDB(dbconn, request.SourceBinDir)
	if err != nil {
		return errors.Wrap(err, "could not retrieve source configuration")
	}

	config.Source = source
	config.Target = &utils.Cluster{BinDir: request.TargetBinDir}
	config.UseLinkMode = request.UseLinkMode

	var ports []int
	for _, p := range request.Ports {
		ports = append(ports, int(p))
	}

	config.TargetPorts, err = assignPorts(config.Source, ports)
	if err != nil {
		return err
	}

	if err := saveConfig(); err != nil {
		return err
	}

	return nil
}

func assignPorts(source *utils.Cluster, ports []int) (PortAssignments, error) {
	if len(ports) == 0 {
		return defaultTargetPorts(source), nil
	}

	ports = sanitize(ports)
	if err := checkTargetPorts(source, ports); err != nil {
		return PortAssignments{}, err
	}

	// Pop the first port off for master.
	masterPort := ports[0]
	ports = ports[1:]

	var standbyPort int
	if _, ok := source.Mirrors[-1]; ok {
		// Pop the next port off for standby.
		standbyPort = ports[0]
		ports = ports[1:]
	}

	return PortAssignments{
		Master:    masterPort,
		Standby:   standbyPort,
		Primaries: ports,
	}, nil
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
func defaultTargetPorts(source *utils.Cluster) PortAssignments {
	// Partition segments by host in order to correctly assign ports.
	segmentsByHost := make(map[string][]utils.SegConfig)

	for content, segment := range source.Primaries {
		// Exclude the master for now. We want to give it its own reserved port,
		// which does not overlap with the other segments, so we'll add it back
		// later.
		if content == -1 {
			continue
		}
		segmentsByHost[segment.Hostname] = append(segmentsByHost[segment.Hostname], segment)
	}

	const masterPort = 50432
	nextPort := masterPort + 1

	var standbyPort int
	if _, ok := source.Mirrors[-1]; ok {
		// Reserve another port for the standby.
		standbyPort = nextPort
		nextPort++
	}

	// Reserve enough ports to handle the host with the most segments.
	var maxSegs int
	for _, segments := range segmentsByHost {
		if len(segments) > maxSegs {
			maxSegs = len(segments)
		}
	}

	var primaryPorts []int
	for i := 0; i < maxSegs; i++ {
		primaryPorts = append(primaryPorts, nextPort)
		nextPort++
	}

	return PortAssignments{
		Master:    masterPort,
		Standby:   standbyPort,
		Primaries: primaryPorts,
	}
}

// checkTargetPorts ensures that the temporary port range passed by the user has
// enough ports to cover a cluster of the given topology. This function assumes
// the port list has at least one port.
func checkTargetPorts(source *utils.Cluster, desiredPorts []int) error {
	if len(desiredPorts) == 0 {
		// failed precondition
		panic("checkTargetPorts() must be called with at least one port")
	}

	segmentsByHost := make(map[string][]utils.SegConfig)

	numAvailablePorts := len(desiredPorts)
	numAvailablePorts-- // master always takes one

	for content, segment := range source.Primaries {
		// Exclude the master; it's taken care of with the first port.
		if content == -1 {
			continue
		}
		segmentsByHost[segment.Hostname] = append(segmentsByHost[segment.Hostname], segment)
	}

	if _, ok := source.Mirrors[-1]; ok {
		// The standby will take a port from the pool.
		numAvailablePorts--
	}

	for _, segments := range segmentsByHost {
		if numAvailablePorts < len(segments) {
			return errors.New("not enough ports for each segment")
		}
	}

	return nil
}
