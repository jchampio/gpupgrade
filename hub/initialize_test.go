package hub

import (
	"reflect"
	"testing"

	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/greenplum-db/gpupgrade/utils/cluster"
)

func TestAssignPorts(t *testing.T) {

	cases := []struct {
		name string

		cluster  *utils.Cluster
		ports    []uint32
		expected []int
	}{{
		name:    "sorts and deduplicates provided port range",
		cluster: MustCreateCluster(t, []cluster.SegConfig{
			//{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			//{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: "p"},
			//{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast2/seg2", Role: "p"},
		}),
		ports:    []uint32{10, 9, 10, 9, 10, 8},
		expected: []int{8, 9, 10},
	}, {
		name: "uses default port range when port list is empty",
		cluster: MustCreateCluster(t, []cluster.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "mdw", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 3, Hostname: "mdw", DataDir: "/data/dbfast2/seg2", Role: "p"},
			{ContentID: 2, DbID: 4, Hostname: "sdw1", DataDir: "/data/dbfast3/seg3", Role: "p"},
		}),
		ports:    []uint32{},
		expected: []int{50432, 50433, 50434},
	}, {
		name: "gives master its own port regardless of host layout",
		cluster: MustCreateCluster(t, []cluster.SegConfig{
			{ContentID: -1, DbID: 1, Hostname: "mdw", DataDir: "/data/qddir/seg-1", Role: "p"},
			{ContentID: 0, DbID: 2, Hostname: "sdw1", DataDir: "/data/dbfast1/seg1", Role: "p"},
			{ContentID: 1, DbID: 3, Hostname: "sdw1", DataDir: "/data/dbfast2/seg2", Role: "p"},
			{ContentID: 2, DbID: 4, Hostname: "sdw1", DataDir: "/data/dbfast3/seg3", Role: "p"},
		}),
		ports:    []uint32{},
		expected: []int{50432, 50433, 50434, 50435},
	}}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			actual, err := assignPorts(c.cluster, c.ports)
			if err != nil {
				t.Errorf("returned error %+v", err)
			}

			if !reflect.DeepEqual(actual, c.expected) {
				t.Errorf("assignPorts(<cluster>, %v)=%v, want %v", c.ports, actual, c.expected)
			}
		})
	}
}
