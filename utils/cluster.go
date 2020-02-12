package utils

import (
	"fmt"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/utils/cluster"
)

type Cluster struct {
	*cluster.Cluster

	BinDir  string
	Version dbconn.GPDBVersion
}

// ClusterFromDB will create a Cluster by querying the passed DBConn for
// information. You must pass the cluster's binary directory, since it cannot be
// divined from the database.
func ClusterFromDB(conn *dbconn.DBConn, binDir string) (*Cluster, error) {
	err := conn.Connect(1)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't connect to cluster")
	}
	defer conn.Close()

	c := new(Cluster)
	c.Version = conn.Version

	segments, err := cluster.GetSegmentConfiguration(conn)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't retrieve segment configuration")
	}

	c.Cluster, err = cluster.NewCluster(segments)
	if err != nil {
		return nil, err
	}

	c.BinDir = binDir

	return c, nil
}

func (c *Cluster) MasterDataDir() string {
	return c.GetDirForContent(-1)
}

func (c *Cluster) MasterPort() int {
	return c.GetPortForContent(-1)
}

// XXX This does not provide mirror hostnames yet.
func (c *Cluster) GetHostnames() []string {
	hostnameMap := make(map[string]bool, 0)
	for _, seg := range c.Primaries {
		hostnameMap[seg.Hostname] = true
	}
	hostnames := make([]string, 0)
	for host := range hostnameMap {
		hostnames = append(hostnames, host)
	}
	return hostnames
}

func (c *Cluster) PrimaryHostnames() []string {
	hostnames := make(map[string]bool, 0)
	for _, seg := range c.Primaries {
		// Ignore the master.
		if seg.ContentID >= 0 {
			hostnames[seg.Hostname] = true
		}
	}

	var list []string
	for host := range hostnames {
		list = append(list, host)
	}

	return list
}

// ErrUnknownHost can be returned by Cluster.SegmentsOn.
var ErrUnknownHost = xerrors.New("no such host in cluster")

type UnknownHostError struct {
	hostname string
}

func (u UnknownHostError) Error() string {
	return fmt.Sprintf("cluster has no segments on host %q", u.hostname)
}

func (u UnknownHostError) Is(err error) bool {
	return err == ErrUnknownHost
}

// SegmentsOn returns the configurations of segments that are running on a given
// host excluding the master. An error of type ErrUnknownHost will be returned
// for unknown hostnames.
func (c Cluster) SegmentsOn(hostname string) ([]cluster.SegConfig, error) {
	var segments []cluster.SegConfig
	for _, segment := range c.Primaries {
		if segment.Hostname == hostname && segment.ContentID != -1 {
			segments = append(segments, segment)
		}
	}

	if len(segments) == 0 {
		return nil, UnknownHostError{hostname}
	}

	return segments, nil
}

func (c *Cluster) StandbyPort() string {
	return "6101"
}

func (c *Cluster) StandbyHostname() string {
	return "localhost"
}

func (c *Cluster) StandbyDataDirectory() string {
	return filepath.Join(c.MasterDataDir(), "..", "..", "standby")
}
