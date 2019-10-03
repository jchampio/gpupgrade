package commanders

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
	. "github.com/onsi/gomega"
)

// Streams the above stdout/err constants to the corresponding standard file
// descriptors, alternately interleaving five-byte chunks.
func HowManyHubsRunning_0_Main() {
	fmt.Print("0")
}
func HowManyHubsRunning_1_Main() {
	fmt.Print("1")
}
func HowManyHubsRunning_badoutput_Main() {
	fmt.Print("bengie")
}

func GpupgradeHub_good_Main() {
	fmt.Print("Hi, Hub started.")
}

func GpupgradeHub_bad_Main() {
	fmt.Fprint(os.Stderr, "Sorry, Hub could not be started.")
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

var (
	g *GomegaWithT
)

func setup(t *testing.T) {
	g = NewGomegaWithT(t)
	execCommandHubStart = nil
	execCommandHubCount = nil
}

func teardown() {
	execCommandHubStart = exec.Command
	execCommandHubCount = exec.Command
}

func TestNoHubIsAlreadyRunning(t *testing.T) {
	setup(t)
	defer teardown()

	execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_0_Main)
	numHubs, err := HowManyHubsRunning()
	g.Expect(err).To(BeNil())
	g.Expect(numHubs).To(Equal(0))
}

func TestHubIsAlreadyRunning(t *testing.T) {
	setup(t)
	defer teardown()

	execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_1_Main)
	numHubs, err := HowManyHubsRunning()
	g.Expect(err).To(BeNil())
	g.Expect(numHubs).To(Equal(1))
}

func TestHowManyHubsRunningFails(t *testing.T) {
	setup(t)
	defer teardown()

	execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_badoutput_Main)
	_, err := HowManyHubsRunning()
	g.Expect(err).ToNot(BeNil())
}

func TestWeCanStartHub(t *testing.T) {
	setup(t)
	defer teardown()

	execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_0_Main)
	execCommandHubStart = exectest.NewCommand(GpupgradeHub_good_Main)
	err := StartHub()
	g.Expect(err).To(BeNil())
}

func TestStartHubHFails(t *testing.T) {
	setup(t)
	defer teardown()

	execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_badoutput_Main)
	execCommandHubStart = exectest.NewCommand(GpupgradeHub_good_Main)
	err := StartHub()
	g.Expect(err).ToNot(BeNil())
}

func TestStartHubRestartFails(t *testing.T) {
	setup(t)
	defer teardown()

	execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_1_Main)
	execCommandHubStart = exectest.NewCommand(GpupgradeHub_good_Main)
	err := StartHub()
	g.Expect(err).ToNot(BeNil())
}

func TestStartHubBadExec(t *testing.T) {
	setup(t)
	defer teardown()

	execCommandHubCount = exectest.NewCommand(HowManyHubsRunning_0_Main)
	execCommandHubStart = exectest.NewCommand(GpupgradeHub_bad_Main)
	err := StartHub()
	g.Expect(err).ToNot(BeNil())
}
