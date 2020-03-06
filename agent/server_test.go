package agent_test

import (
	"io/ioutil"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"github.com/greenplum-db/gpupgrade/agent"
	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Server", func() {
	var (
		dir       string
		agentConf agent.Config
		exists    func() bool
	)

	BeforeEach(func() {
		testhelper.SetupTestLogger()
		dir, err := ioutil.TempDir("", "")
		Expect(err).ToNot(HaveOccurred())

		agentPort, err := testutils.GetOpenPort()
		Expect(err).ToNot(HaveOccurred())

		agentConf = agent.Config{
			Port:     agentPort,
			StateDir: dir,
		}

		exists = func() bool {
			_, err := os.Stat(dir)
			return !os.IsNotExist(err)
		}
	})

	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
	})

	It("starts if stateDir already exists", func() {
		server := agent.NewServer(agentConf)

		go server.Start()
		defer server.Stop()

		Eventually(exists).Should(BeTrue())
		os.RemoveAll(dir)
	})

	It("creates stateDir if none exists", func() {
		err := os.RemoveAll(dir)
		Expect(err).ToNot(HaveOccurred())
		_, err = os.Stat(dir)
		Expect(os.IsNotExist(err)).To(BeTrue())

		server := agent.NewServer(agentConf)
		go server.Start()
		defer server.Stop()

		Eventually(exists).Should(BeTrue())
		os.RemoveAll(dir)
	})
})
