package integrations_test

import (
	"os/exec"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("gpupgrade_hub", func() {

	// XXX We should be testing the locally built artifacts, and killing only
	// hubs that are started as part of this test. The current logic will break
	// functional installed systems.
	BeforeEach(func() {
		killHub()
	})

	AfterEach(func() {
		killHub()
	})

	It("does not daemonize unless explicitly told to", func() {
		cmd := exec.Command("gpupgrade_hub")
		err := make(chan error, 1)

		go func() {
			// We expect this to never return.
			err <- cmd.Run()
		}()

		Consistently(err).ShouldNot(Receive())
	})
})
