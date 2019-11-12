package services

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/testutils/exectest"
	"github.com/greenplum-db/gpupgrade/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// Does nothing.
func EmptyMain() {}

// Prints the environment, one variable per line, in NAME=VALUE format.
func EnvironmentMain() {
	for _, e := range os.Environ() {
		fmt.Println(e)
	}
}

func init() {
	exectest.RegisterMains(
		EmptyMain,
		EnvironmentMain,
	)
}

var _ = Describe("UpgradeSegments", func() {
	var (
		segments     []Segment
		sourceBinDir string
		targetBinDir string
	)

	BeforeEach(func() {
		sourceBinDir = "/old/bin"
		targetBinDir = "/new/bin"

		segments = []Segment{
			{UpgradeDir: "", DataDirPair: &idl.DataDirPair{OldDataDir: "old/datadir1", NewDataDir: "new/datadir1", Content: 0, OldPort: 1, NewPort: 11}},
			// TODO: Add a way to test multiple calls to execCommand.
			//{UpgradeDir: "", DataDirPair: &idl.DataDirPair{OldDataDir: "old/datadir2", NewDataDir: "new/datadir2", Content: 1, OldPort: 2, NewPort: 22}},
		}

		utils.System.MkdirAll = func(string, os.FileMode) error {
			return nil
		}

		// Disable exec.Command. This way, if a test forgets to mock it out, we
		// crash the test instead of executing code on a dev system.
		execCommand = nil
	})

	AfterEach(func() {
		execCommand = exec.Command
	})

	It("calls pg_upgrade with the expected options", func() {
		execCommand = exectest.NewCommandWithVerifier(EmptyMain,
			func(path string, args ...string) {
				// pg_upgrade should be run from the target installation.
				expectedPath := filepath.Join(targetBinDir, "pg_upgrade")
				Expect(path).To(Equal(expectedPath))

				// Check the arguments. We use a FlagSet so as not to couple
				// against option order.
				var fs flag.FlagSet

				oldBinDir := fs.String("old-bindir", "", "")
				newBinDir := fs.String("new-bindir", "", "")
				oldDataDir := fs.String("old-datadir", "", "")
				newDataDir := fs.String("new-datadir", "", "")
				oldPort := fs.Int("old-port", -1, "")
				newPort := fs.Int("new-port", -1, "")
				mode := fs.String("mode", "", "")
				link := fs.Bool("link", false, "")

				err := fs.Parse(args)
				Expect(err).NotTo(HaveOccurred())

				Expect(*oldBinDir).To(Equal(sourceBinDir))
				Expect(*newBinDir).To(Equal(targetBinDir))
				Expect(*oldDataDir).To(Equal(segments[0].OldDataDir))
				Expect(*newDataDir).To(Equal(segments[0].NewDataDir))
				Expect(*oldPort).To(Equal(int(segments[0].OldPort)))
				Expect(*newPort).To(Equal(int(segments[0].NewPort)))
				Expect(*mode).To(Equal("segment"))
				Expect(*link).To(BeTrue())

				// No other arguments should be passed.
				Expect(fs.Args()).To(BeEmpty())
			})

		err := UpgradeSegments("/old/bin", "/new/bin", segments)
		Expect(err).NotTo(HaveOccurred())
	})

	It("unsets PGPORT and PGHOST", func() {
		// Set our environment.
		os.Setenv("PGPORT", "5432")
		os.Setenv("PGHOST", "localhost")
		defer func() {
			os.Unsetenv("PGPORT")
			os.Unsetenv("PGHOST")
		}()

		// Echo the environment to stdout.
		execCommand = exectest.NewCommand(EnvironmentMain)

		var buf bytes.Buffer
		err := UpgradeSegments("/old/bin", "/new/bin", segments)
		Expect(err).NotTo(HaveOccurred())

		scanner := bufio.NewScanner(&buf)
		for scanner.Scan() {
			Expect(scanner.Text()).NotTo(HavePrefix("PGPORT="),
				"PGPORT was not stripped from the child environment")
			Expect(scanner.Text()).NotTo(HavePrefix("PGHOST="),
				"PGHOST was not stripped from the child environment")
		}
		Expect(scanner.Err()).NotTo(HaveOccurred())
	})
})
