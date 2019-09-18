package commanders

import (
	"errors"
	"os"
	"os/exec"

	"github.com/golang/mock/gomock"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/idl/mock_idl"
	"github.com/greenplum-db/gpupgrade/testutils/exectest"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// FIXME: we qualify ginkgo here because of commanders.Reporter
//    which collides with gomega.Reporter

// Streams the above stdout/err constants to the corresponding standard file
// descriptors, alternately interleaving five-byte chunks.
func HowManyHubsRunning_0_Main() {
	os.Stdout.WriteString("0")
}
func HowManyHubsRunning_1_Main() {
	os.Stdout.WriteString("1")
}
func HowManyHubsRunning_badoutput_Main() {
	os.Stdout.WriteString("bengie")
}

func GpupgradeHub_good_Main() {
	os.Stdout.WriteString("Hi, Hub started.")
}

func GpupgradeHub_bad_Main() {
	os.Stderr.WriteString("Sorry, Hub could not be started.")
	os.Exit(1)
}

func init() {
	exectest.RegisterMains(
		HowManyHubsRunning_0_Main,
		HowManyHubsRunning_1_Main,
		HowManyHubsRunning_badoutput_Main,
		GpupgradeHub_good_Main,
		GpupgradeHub_bad_Main,
	)
}

var _ = ginkgo.Describe("Initialize", func() {

	var (
		ctrl *gomock.Controller
	)

	ginkgo.BeforeEach(func() {
		ctrl = gomock.NewController(ginkgo.GinkgoT())

		// Disable exec.Command. This way, if a test forgets to mock it out, we
		// crash the test instead of executing code on a dev system.
		execCommandHubStart = nil
		execCommandHubCount = nil

	})

	ginkgo.AfterEach(func() {
		ctrl.Finish()
		execCommandHubStart = exec.Command
		execCommandHubCount = exec.Command
	})

	ginkgo.It("there is no hub already running", func() {
		execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_0_Main)
		numHubs, err := HowManyHubsRunning()
		Expect(err).NotTo(HaveOccurred())
		Expect(numHubs).To(Equal(0))
	})

	ginkgo.It("there is a hub already running", func() {
		execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_1_Main)
		numHubs, err := HowManyHubsRunning()
		Expect(err).NotTo(HaveOccurred())
		Expect(numHubs).To(Equal(1))
	})

	ginkgo.It("we get garbage when we request how many hubs are running", func() {
		execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_badoutput_Main)
		numHubs, err := HowManyHubsRunning()
		Expect(err).To(HaveOccurred())
		Expect(numHubs).To(Equal(-1))
	})

	ginkgo.It("we can start the hub", func() {
		execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_0_Main)
		execCommandHubStart = exectest.NewCommand(GpupgradeHub_good_Main)
		err := StartHub("/bindir")
		Expect(err).NotTo(HaveOccurred())
	})

	ginkgo.It("we cannot start hub when if we cannot determine if hub is already running", func() {
		execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_badoutput_Main)
		execCommandHubStart = exectest.NewCommand(GpupgradeHub_good_Main)
		err := StartHub("/bindir")
		Expect(err).To(HaveOccurred())
	})

	ginkgo.It("we cannot start hub if hub is already running", func() {
		execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_1_Main)
		execCommandHubStart = exectest.NewCommand(GpupgradeHub_good_Main)
		err := StartHub("/bindir")
		Expect(err).To(HaveOccurred())
	})

	ginkgo.It("we cannot start hub because exec failed", func() {
		execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_0_Main)
		execCommandHubStart = exectest.NewCommand(GpupgradeHub_bad_Main)
		err := StartHub("/bindir")
		Expect(err).To(HaveOccurred())
	})

	ginkgo.It("we can initialize", func() {
		client := mock_idl.NewMockCliToHubClient(ctrl)
		client.EXPECT().Initialize(
			gomock.Any(),
			&idl.InitializeRequest{OldBinDir: "olddir", NewBinDir: "newdir", OldPort: 22},
		).Return(&idl.InitializeReply{}, nil)

		err := Initialize(client, "olddir", "newdir", 22)
		Expect(err).To(BeNil())
	})

	ginkgo.It("we cannot initialize", func() {
		client := mock_idl.NewMockCliToHubClient(ctrl)
		client.EXPECT().Initialize(
			gomock.Any(),
			&idl.InitializeRequest{OldBinDir: "olddir", NewBinDir: "newdir", OldPort: 22},
		).Return(&idl.InitializeReply{}, errors.New("something failed with gRPC"))

		err := Initialize(client, "olddir", "newdir", 22)
		Expect(err).ToNot(BeNil())
	})

})
