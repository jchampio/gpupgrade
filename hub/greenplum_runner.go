package hub

import (
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/kballard/go-shellquote"
)

type GreenplumRunner interface {
	ShellRunner
	BinDir() string
	MasterDataDirectory() string
	MasterPort() int
}

func (e *greenplumRunner) Run(utilityName string, arguments ...string) error {
	path := filepath.Join(e.binDir, utilityName)

	arguments = append([]string{path}, arguments...)
	script := shellquote.Join(arguments...)

	withGreenplumPath := fmt.Sprintf("source %s/../greenplum_path.sh && %s", e.binDir, script)

	command := exec.Command("bash", "-c", withGreenplumPath)
	command.Env = append(command.Env, fmt.Sprintf("%v=%v", "MASTER_DATA_DIRECTORY", e.masterDataDirectory))
	command.Env = append(command.Env, fmt.Sprintf("%v=%v", "PGPORT", e.masterPort))
	output, err := command.CombinedOutput()

	fmt.Printf("Master data directory, %v\n", e.masterDataDirectory)
	fmt.Printf("%s: %s \n", script, string(output))

	return err
}

type greenplumRunner struct {
	binDir              string
	masterDataDirectory string
	masterPort          int
}

func (e *greenplumRunner) BinDir() string {
	return e.binDir
}

func (e *greenplumRunner) MasterDataDirectory() string {
	return e.masterDataDirectory
}

func (e *greenplumRunner) MasterPort() int {
	return e.masterPort
}
