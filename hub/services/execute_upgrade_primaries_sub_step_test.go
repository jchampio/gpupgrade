package services_test

import (
	"errors"
	"path/filepath"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gp-common-go-libs/cluster"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/idl/mock_idl"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub.ExecuteUpgradePrimariesSubStep()", func() {
	var (
		ctrl       *gomock.Controller
		mockStream *mock_idl.MockCliToHub_ExecuteServer
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		mockStream = mock_idl.NewMockCliToHub_ExecuteServer(ctrl)
	})

	AfterEach(func() {
		ctrl.Finish()
	})

	It("returns nil error, and agent receives only expected segmentConfig values", func() {
		mockStream.EXPECT().
			Send(gomock.Any()).
			AnyTimes()

		seg1 := target.Segments[0]
		seg1.DataDir = filepath.Join(dir, "seg1_upgrade")
		seg1.Port = 27432
		target.Segments[0] = seg1

		seg2 := target.Segments[1]
		seg2.DataDir = filepath.Join(dir, "seg2_upgrade")
		seg2.Port = 27433

		// Set up both segments to be on the same host (but still distinct from
		// the master host).
		seg2.Hostname = seg1.Hostname
		target.Segments[1] = seg2

		// Source hostnames must match the target.
		sourceSeg2 := source.Segments[1]
		sourceSeg2.Hostname = seg2.Hostname
		source.Segments[1] = sourceSeg2

		err := hub.ExecuteUpgradePrimariesSubStep(mockStream)
		Expect(err).ToNot(HaveOccurred())

		Expect(mockAgent.UpgradeConvertPrimarySegmentsRequest.OldBinDir).To(Equal("/source/bindir"))
		Expect(mockAgent.UpgradeConvertPrimarySegmentsRequest.NewBinDir).To(Equal("/target/bindir"))
		Expect(mockAgent.UpgradeConvertPrimarySegmentsRequest.DataDirPairs).To(ConsistOf([]*idl.DataDirPair{
			{OldDataDir: filepath.Join(dir, "seg1"), NewDataDir: filepath.Join(dir, "seg1_upgrade"), Content: 0, OldPort: 25432, NewPort: 27432},
			{OldDataDir: filepath.Join(dir, "seg2"), NewDataDir: filepath.Join(dir, "seg2_upgrade"), Content: 1, OldPort: 25433, NewPort: 27433},
		}))
	})

	It("returns an error if new config does not contain all the same content as the old config", func() {
		mockStream.EXPECT().
			Send(gomock.Any()).
			AnyTimes()

		target.Cluster = &cluster.Cluster{
			ContentIDs: []int{0},
			Segments: map[int]cluster.SegConfig{
				0: newSegment(0, "localhost", "new/datadir1", 11),
			},
		}

		err := hub.ExecuteUpgradePrimariesSubStep(mockStream)
		Expect(err).To(HaveOccurred())
		Expect(mockAgent.NumberOfCalls()).To(Equal(0))
	})

	It("returns an error if the content matches, but the hostname does not", func() {
		mockStream.EXPECT().
			Send(gomock.Any()).
			AnyTimes()

		differentSeg := target.Segments[0]
		differentSeg.Hostname = "localhost2"
		target.Segments[0] = differentSeg

		err := hub.ExecuteUpgradePrimariesSubStep(mockStream)
		Expect(err).To(HaveOccurred())

		Expect(mockAgent.NumberOfCalls()).To(Equal(0))
	})

	It("returns an error if any upgrade primary call to any agent fails", func() {
		mockStream.EXPECT().
			Send(gomock.Any()).
			AnyTimes()

		mockAgent.Err <- errors.New("fail upgrade primary call")

		err := hub.ExecuteUpgradePrimariesSubStep(mockStream)
		Expect(err).To(HaveOccurred())

		Expect(mockAgent.NumberOfCalls()).To(Equal(2))
	})
})

func newSegment(content int, hostname, dataDir string, port int) cluster.SegConfig {
	return cluster.SegConfig{
		ContentID: content,
		Hostname:  hostname,
		DataDir:   dataDir,
		Port:      port,
	}
}
