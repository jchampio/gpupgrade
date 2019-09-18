package commanders_test

import (
	"github.com/greenplum-db/gpupgrade/testutils/exectest"
	"os"
	"testing"

	// "github.com/greenplum-db/gp-common-go-libs/gplog"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestCommands(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Commanders Suite")
}

func TestMain(m *testing.M) {
	os.Exit(exectest.Run(m))
}

// Activate me once we start running unit tests. At that time, specify a better logging directory for unit test output
// var _ = BeforeSuite(func() {
// 	gplog.InitializeLogging("commanders unit tests", "")
// })
