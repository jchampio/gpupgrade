package greenplum_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/greenplum-db/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/testutils"
)

func TestCluster(t *testing.T) {
	primaries := map[int]greenplum.SegConfig{
		-1: {DbID: 1, ContentID: -1, Port: 5432, Hostname: "localhost", DataDir: "/data/gpseg-1"},
		0:  {DbID: 2, ContentID: 0, Port: 20000, Hostname: "localhost", DataDir: "/data/gpseg0"},
		2:  {DbID: 4, ContentID: 2, Port: 20002, Hostname: "localhost", DataDir: "/data/gpseg2"},
		3:  {DbID: 5, ContentID: 3, Port: 20003, Hostname: "remotehost2", DataDir: "/data/gpseg3"},
	}
	for content, seg := range primaries {
		seg.Role = greenplum.PrimaryRole
		primaries[content] = seg
	}

	mirrors := map[int]greenplum.SegConfig{
		-1: {DbID: 8, ContentID: -1, Port: 5433, Hostname: "localhost", DataDir: "/mirror/gpseg-1"},
		0:  {DbID: 3, ContentID: 0, Port: 20001, Hostname: "localhost", DataDir: "/mirror/gpseg0"},
		2:  {DbID: 6, ContentID: 2, Port: 20004, Hostname: "localhost", DataDir: "/mirror/gpseg2"},
		3:  {DbID: 7, ContentID: 3, Port: 20005, Hostname: "remotehost2", DataDir: "/mirror/gpseg3"},
	}
	for content, seg := range mirrors {
		seg.Role = greenplum.MirrorRole
		mirrors[content] = seg
	}

	master := primaries[-1]
	standby := mirrors[-1]

	cases := []struct {
		name      string
		primaries []greenplum.SegConfig
		mirrors   []greenplum.SegConfig
	}{
		{"mirrorless single-host, single-segment", []greenplum.SegConfig{master, primaries[0]}, nil},
		{"mirrorless single-host, multi-segment", []greenplum.SegConfig{master, primaries[0], primaries[2]}, nil},
		{"mirrorless multi-host, multi-segment", []greenplum.SegConfig{master, primaries[0], primaries[3]}, nil},
		{"single-host, single-segment",
			[]greenplum.SegConfig{master, primaries[0]},
			[]greenplum.SegConfig{mirrors[0]},
		},
		{"single-host, multi-segment",
			[]greenplum.SegConfig{master, primaries[0], primaries[2]},
			[]greenplum.SegConfig{mirrors[0], mirrors[2]},
		},
		{"multi-host, multi-segment",
			[]greenplum.SegConfig{master, primaries[0], primaries[3]},
			[]greenplum.SegConfig{mirrors[0], mirrors[3]},
		},
		{"multi-host, multi-segment with standby",
			[]greenplum.SegConfig{master, primaries[0], primaries[3]},
			[]greenplum.SegConfig{standby, mirrors[0], mirrors[3]},
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s cluster", c.name), func(t *testing.T) {
			segments := append(c.primaries, c.mirrors...)

			actualCluster, err := greenplum.NewCluster(segments)
			if err != nil {
				t.Fatalf("returned error %+v", err)
			}

			actualContents := actualCluster.GetContentList()

			var expectedContents []int
			for _, p := range c.primaries {
				expectedContents = append(expectedContents, p.ContentID)
			}

			if !reflect.DeepEqual(actualContents, expectedContents) {
				t.Errorf("GetContentList() = %v, want %v", actualContents, expectedContents)
			}

			for _, expected := range c.primaries {
				content := expected.ContentID

				actual := actualCluster.Primaries[content]
				if actual != expected {
					t.Errorf("Primaries[%d] = %+v, want %+v", content, actual, expected)
				}

				host := actualCluster.GetHostForContent(content)
				if host != expected.Hostname {
					t.Errorf("GetHostForContent(%d) = %q, want %q", content, host, expected.Hostname)
				}

				port := actualCluster.GetPortForContent(content)
				if port != expected.Port {
					t.Errorf("GetPortForContent(%d) = %d, want %d", content, port, expected.Port)
				}

				dbid := actualCluster.GetDbidForContent(content)
				if dbid != expected.DbID {
					t.Errorf("GetDbidForContent(%d) = %d, want %d", content, dbid, expected.DbID)
				}

				datadir := actualCluster.GetDirForContent(content)
				if datadir != expected.DataDir {
					t.Errorf("GetDirForContent(%d) = %q, want %q", content, datadir, expected.DataDir)
				}
			}

			for _, expected := range c.mirrors {
				content := expected.ContentID

				actual := actualCluster.Mirrors[content]
				if actual != expected {
					t.Errorf("Mirrors[%d] = %+v, want %+v", content, actual, expected)
				}
			}
		})
	}

	errCases := []struct {
		name     string
		segments []greenplum.SegConfig
	}{
		{"bad role", []greenplum.SegConfig{
			{Role: "x"},
		}},
		{"mirror without primary", []greenplum.SegConfig{
			{ContentID: 0, Role: "p"},
			{ContentID: 1, Role: "m"},
		}},
		{"duplicated primary contents", []greenplum.SegConfig{
			{ContentID: 0, Role: "p"},
			{ContentID: 0, Role: "p"},
		}},
		{"duplicated mirror contents", []greenplum.SegConfig{
			{ContentID: 0, Role: "p"},
			{ContentID: 0, Role: "m"},
			{ContentID: 0, Role: "m"},
		}},
	}

	for _, c := range errCases {
		t.Run(fmt.Sprintf("doesn't allow %s", c.name), func(t *testing.T) {
			_, err := greenplum.NewCluster(c.segments)

			if !xerrors.Is(err, greenplum.ErrInvalidSegments) {
				t.Errorf("returned error %#v, want %#v", err, greenplum.ErrInvalidSegments)
			}
		})
	}
}

// TODO: these tests would be better served executing against an actual SQL
// engine rather than mocking out a specific implementation.
func TestGetSegmentConfiguration(t *testing.T) {
	testhelper.SetupTestLogger() // init gplog

	cases := []struct {
		name     string
		rows     [][]driver.Value
		expected []greenplum.SegConfig
	}{{
		"single-host, single-segment",
		[][]driver.Value{
			{"1", "0", "15432", "localhost", "/data/gpseg0", "p"},
		},
		[]greenplum.SegConfig{
			{DbID: 1, ContentID: 0, Port: 15432, Hostname: "localhost", DataDir: "/data/gpseg0", Role: "p"},
		},
	}, {
		"single-host, multi-segment",
		[][]driver.Value{
			{"1", "0", "15432", "localhost", "/data/gpseg0", "p"},
			{"2", "1", "15433", "localhost", "/data/gpseg1", "p"},
		},
		[]greenplum.SegConfig{
			{DbID: 1, ContentID: 0, Port: 15432, Hostname: "localhost", DataDir: "/data/gpseg0", Role: "p"},
			{DbID: 2, ContentID: 1, Port: 15433, Hostname: "localhost", DataDir: "/data/gpseg1", Role: "p"},
		},
	}, {
		"multi-host, multi-segment",
		[][]driver.Value{
			{"1", "0", "15432", "localhost", "/data/gpseg0", "p"},
			{"2", "1", "15433", "localhost", "/data/gpseg1", "p"},
			{"3", "2", "15434", "remotehost", "/data/gpseg2", "m"},
		},
		[]greenplum.SegConfig{
			{DbID: 1, ContentID: 0, Port: 15432, Hostname: "localhost", DataDir: "/data/gpseg0", Role: "p"},
			{DbID: 2, ContentID: 1, Port: 15433, Hostname: "localhost", DataDir: "/data/gpseg1", Role: "p"},
			{DbID: 3, ContentID: 2, Port: 15434, Hostname: "remotehost", DataDir: "/data/gpseg2", Role: "m"},
		},
	}}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s cluster", c.name), func(t *testing.T) {
			// Set up the connection to return the expected rows.
			rows := sqlmock.NewRows([]string{"dbid", "contentid", "port", "hostname", "datadir", "role"})
			for _, row := range c.rows {
				rows.AddRow(row...)
			}

			db, ctrl := sqlmockDB(t)
			defer ctrl.Finish()

			ctrl.ExpectQuery("SELECT (.*)").WillReturnRows(rows)

			results, err := greenplum.GetSegmentConfiguration(db, dbconn.NewVersion("6.0.0"))
			if err != nil {
				t.Errorf("returned error %+v", err)
			}

			if !reflect.DeepEqual(results, c.expected) {
				t.Errorf("got configuration %+v, want %+v", results, c.expected)
			}
		})
	}
}

func TestPrimaryHostnames(t *testing.T) {
	testStateDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Errorf("got error when creating tempdir: %+v", err)
	}
	expectedCluster := testutils.CreateMultinodeSampleCluster("/tmp")
	// todo: pass args for bindir / version to CreateMultinodeSampleCluster
	expectedCluster.BinDir = "/fake/path"
	expectedCluster.Version = dbconn.NewVersion("6.0.0")
	testhelper.SetupTestLogger()

	defer func() {
		os.RemoveAll(testStateDir)
	}()

	t.Run("returns a list of hosts for only the primaries", func(t *testing.T) {
		actual := expectedCluster.PrimaryHostnames()
		sort.Strings(actual)

		expected := []string{"host1", "host2"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected hostnames: %#v got: %#v", expected, actual)
		}
	})
}

func TestClusterFromDB(t *testing.T) {
	t.Run("returns an error if the segment configuration query fails", func(t *testing.T) {
		db, ctrl := sqlmockDB(t)
		defer ctrl.Finish()

		expectVersionQuery(ctrl, "5.3.4")

		queryErr := errors.New("failed to get segment configuration")
		ctrl.ExpectQuery("SELECT .* FROM gp_segment_configuration").
			WillReturnError(queryErr)

		_, err := greenplum.ClusterFromDB(db, "")
		if err == nil {
			t.Errorf("Expected an error, but got nil")
		}

		if !xerrors.Is(err, queryErr) {
			t.Errorf("returned error %#v, want %#v", err, queryErr)
		}
	})

	t.Run("populates a cluster using DB information", func(t *testing.T) {
		db, ctrl := sqlmockDB(t)
		defer ctrl.Finish()

		versionRows := sqlmock.NewRows([]string{"version"}).
			AddRow("PostgreSQL 8.3.24 (Greenplum Database 5.3.4) on x86_64-apple-darwin18.7.0, compiled by Apple clang version 11.0.0 (clang-1100.0.33.17), 64-bit compiled on Mar 11 2020 12:10:06")

		ctrl.ExpectQuery("SELECT version()").
			WillReturnRows(versionRows)

		ctrl.ExpectQuery("SELECT .* FROM gp_segment_configuration").
			WillReturnRows(testutils.MockSegmentConfiguration())

		binDir := "/usr/local/gpdb/bin"

		actualCluster, err := greenplum.ClusterFromDB(db, binDir)
		if err != nil {
			t.Errorf("got unexpected error: %+v", err)
		}

		expectedCluster := testutils.MockCluster()
		expectedCluster.Version = dbconn.NewVersion("5.3.4")
		expectedCluster.BinDir = binDir

		if !reflect.DeepEqual(actualCluster, expectedCluster) {
			t.Errorf("expected: %#v got: %#v", expectedCluster, actualCluster)
		}
	})
}

func TestSelectSegments(t *testing.T) {
	segs := []greenplum.SegConfig{
		{ContentID: 1, Role: "p"},
		{ContentID: 2, Role: "p"},
		{ContentID: 3, Role: "p"},
		{ContentID: 3, Role: "m"},
	}
	cluster, err := greenplum.NewCluster(segs)
	if err != nil {
		t.Fatalf("creating test cluster: %+v", err)
	}

	// Ensure all segments are visited correctly.
	selectAll := func(_ *greenplum.SegConfig) bool { return true }
	results := cluster.SelectSegments(selectAll)

	if !reflect.DeepEqual(results, segs) {
		t.Errorf("SelectSegments(*) = %+v, want %+v", results, segs)
	}

	// Test a simple selector.
	moreThanOne := func(c *greenplum.SegConfig) bool { return c.ContentID > 1 }
	results = cluster.SelectSegments(moreThanOne)

	expected := []greenplum.SegConfig{segs[1], segs[2], segs[3]}
	if !reflect.DeepEqual(results, expected) {
		t.Errorf("SelectSegments(ContentID > 1) = %+v, want %+v", results, expected)
	}

}
