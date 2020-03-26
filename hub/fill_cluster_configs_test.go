package hub

import (
	"reflect"
	"testing"

	cluster2 "github.com/greenplum-db/gpupgrade/greenplum" // TODO: replace with greenplum
	"github.com/greenplum-db/gpupgrade/upgrade"
)

func TestAssignDataDirsAndPorts(t *testing.T) {
	var upgradeID upgrade.ID

	expectedDataDir := func(sourceDir string) string {
		return upgrade.TempDataDir(sourceDir, "seg", upgradeID)
	}

	cases := []struct {
		name string

		cluster  *cluster2.Cluster
		ports    []int
		expected InitializeConfig
	}{{
		name: "sorts and deduplicates provided port range",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast2/seg2", Role: "p"},
		}),
		ports: []int{10, 9, 10, 9, 10, 8},
		expected: InitializeConfig{
			Master: cluster2.SegConfig{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: "p", Port: 8},
			Primaries: []cluster2.SegConfig{
				{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: "p", Port: 9},
				{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast2/seg2"), Role: "p", Port: 10},
			}},
	}, {
		name: "uses default port range when port list is empty",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast2/seg2", Role: "p"},
			{ContentID: 2, DbID: 4, Hostname: "sdw1", DataDir: "/data/dbfast3/seg3", Role: "p"},
		}),
		ports: []int{},
		expected: InitializeConfig{
			Master: cluster2.SegConfig{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: "p", Port: 50432},
			Primaries: []cluster2.SegConfig{
				{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: "p", Port: 50433},
				{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast2/seg2"), Role: "p", Port: 50434},
				{ContentID: 2, DbID: 4, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast3/seg3"), Role: "p", Port: 50433},
			}},
	}, {
		name: "gives master its own port regardless of host layout",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast2/seg2", Role: "p"},
			{ContentID: 2, DbID: 4, Hostname: "sdw1", DataDir: "/data/dbfast3/seg3", Role: "p"},
		}),
		ports: []int{},
		expected: InitializeConfig{
			Master: cluster2.SegConfig{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: "p", Port: 50432},
			Primaries: []cluster2.SegConfig{
				{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: "p", Port: 50433},
				{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast2/seg2"), Role: "p", Port: 50434},
				{ContentID: 2, DbID: 4, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast3/seg3"), Role: "p", Port: 50435},
			}},
	}, {
		name: "when using default ports, it sets up mirrors as expected in the InitializeConfig",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast2/seg2", Role: "p"},
			{ContentID: 0, DbID: 4, Hostname: "sdw1", DataDir: "/data/dbfast_mirror1/seg1", Role: "m"},
			{ContentID: 1, DbID: 5, Hostname: "sdw1", DataDir: "/data/dbfast_mirror2/seg2", Role: "m"},
		}),
		ports: []int{},
		expected: InitializeConfig{
			Master: cluster2.SegConfig{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: "p", Port: 50432},
			Primaries: []cluster2.SegConfig{
				{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: "p", Port: 50433},
				{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast2/seg2"), Role: "p", Port: 50434},
			},
			Mirrors: []cluster2.SegConfig{
				{ContentID: 0, DbID: 4, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast_mirror1/seg1"), Role: "m", Port: 50435},
				{ContentID: 1, DbID: 5, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast_mirror2/seg2"), Role: "m", Port: 50436},
			},
		},
	}, {
		name: "provides a standby port",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Hostname: "smdw", DataDir: "/data/standby", Role: "m"},
			{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
		}),
		ports: []int{},
		expected: InitializeConfig{
			Master:    cluster2.SegConfig{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: "p", Port: 50432},
			Standby:   cluster2.SegConfig{ContentID: -1, DbID: 2, Hostname: "smdw", DataDir: expectedDataDir("/data/standby"), Role: "m", Port: 50433},
			Primaries: []cluster2.SegConfig{{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: "p", Port: 50434}},
		},
	}, {
		name: "deals with master and standby on the same host",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/standby", Role: "m"},
			{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
		}),
		ports: []int{},
		expected: InitializeConfig{
			Master:    cluster2.SegConfig{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: "p", Port: 50432},
			Standby:   cluster2.SegConfig{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/standby"), Role: "m", Port: 50433},
			Primaries: []cluster2.SegConfig{{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: "p", Port: 50434}},
		},
	}, {
		name: "deals with master and standby on the same host as other segments",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/standby", Role: "m"},
			{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: "p"},
		}),
		ports: []int{},
		expected: InitializeConfig{
			Master:    cluster2.SegConfig{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: "p", Port: 50432},
			Standby:   cluster2.SegConfig{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/standby"), Role: "m", Port: 50433},
			Primaries: []cluster2.SegConfig{{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: "p", Port: 50434}},
		},
	}, {
		name: "assigns provided ports to the standby",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/standby", Role: "m"},
			{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: "p"},
		}),
		ports: []int{1, 2, 3},
		expected: InitializeConfig{
			Master:    cluster2.SegConfig{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: "p", Port: 1},
			Standby:   cluster2.SegConfig{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/standby"), Role: "m", Port: 2},
			Primaries: []cluster2.SegConfig{{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: "p", Port: 3}},
		},
	}, {
		name: "assigns provided ports to cluster with standby and multiple primaries and multiple mirrors",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/standby", Role: "m"},
			{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 4, Hostname: "sdw2", DataDir: "/data/dbfast2/seg2", Role: "p"},
			{ContentID: 2, DbID: 5, Hostname: "sdw3", DataDir: "/data/dbfast3/seg3", Role: "p"},
			{ContentID: 0, DbID: 6, Hostname: "sdw2", DataDir: "/data/dbfast_mirror1/seg1", Role: "m"},
			{ContentID: 1, DbID: 7, Hostname: "sdw3", DataDir: "/data/dbfast_mirror2/seg2", Role: "m"},
			{ContentID: 2, DbID: 8, Hostname: "sdw1", DataDir: "/data/dbfast_mirror3/seg3", Role: "m"},
		}),
		ports: []int{1, 2, 3, 4, 5},
		expected: InitializeConfig{
			Master:  cluster2.SegConfig{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: expectedDataDir("/data/qddir/seg-1"), Role: "p", Port: 1},
			Standby: cluster2.SegConfig{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: expectedDataDir("/data/standby"), Role: "m", Port: 2},
			Primaries: []cluster2.SegConfig{
				{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast1/seg1"), Role: "p", Port: 3},
				{ContentID: 1, DbID: 4, Hostname: "sdw2", DataDir: expectedDataDir("/data/dbfast2/seg2"), Role: "p", Port: 3},
				{ContentID: 2, DbID: 5, Hostname: "sdw3", DataDir: expectedDataDir("/data/dbfast3/seg3"), Role: "p", Port: 3},
			},
			Mirrors: []cluster2.SegConfig{
				{ContentID: 0, DbID: 6, Hostname: "sdw2", DataDir: expectedDataDir("/data/dbfast_mirror1/seg1"), Role: "m", Port: 4},
				{ContentID: 1, DbID: 7, Hostname: "sdw3", DataDir: expectedDataDir("/data/dbfast_mirror2/seg2"), Role: "m", Port: 4},
				{ContentID: 2, DbID: 8, Hostname: "sdw1", DataDir: expectedDataDir("/data/dbfast_mirror3/seg3"), Role: "m", Port: 4},
			},
		},
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual, err := AssignDatadirsAndPorts(c.cluster, c.ports, upgradeID)
			if err != nil {
				t.Errorf("returned error %+v", err)
			}

			if !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("AssignDatadirsAndPorts(<cluster>, %v)=%v, want %v", c.ports, actual, c.expected)
			}
		})
	}

	errCases := []struct {
		name string

		cluster *cluster2.Cluster
		ports   []int
	}{{
		name: "errors when not given enough ports (single host)",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast2/seg2", Role: "p"},
		}),
		ports: []int{15433},
	}, {
		name: "errors when not given enough ports (multiple hosts)",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast2/seg2", Role: "p"},
		}),
		ports: []int{15433, 25432},
	}, {
		name: "errors when not given enough unique ports",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast2/seg2", Role: "p"},
		}),
		ports: []int{15433, 15433, 15433},
	}, {
		name: "errors when not given enough unique ports with a standby",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "m"},
			{ContentID: 0, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: "p"},
		}),
		ports: []int{15433, 15434},
	}, {
		name: "errors when not given enough unique ports with a standby on a different host",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: "m"},
			{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
		}),
		ports: []int{15433, 15434},
	}, {
		name: "errors when there are not enough ports for the mirrors",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast_mirror1/seg1", Role: "m"},
		}),
		ports: []int{15433, 15434},
	}, {
		// regression case
		name: "doesn't panic when not given enough unique ports with a standby on a different host",
		cluster: MustCreateCluster(t, []cluster2.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: -1, DbID: 2, Hostname: "smdw", DataDir: "/data/qddir/seg-1", Role: "m"},
			{ContentID: 0, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
		}),
		ports: []int{15433},
	}}

	for _, c := range errCases {
		t.Run(c.name, func(t *testing.T) {
			_, err := AssignDatadirsAndPorts(c.cluster, c.ports, 0)
			if err == nil {
				t.Errorf("AssignDatadirsAndPorts(<cluster>, %v) returned nil, want error", c.ports)
			}
		})
	}
}
