package hub

import (
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/kballard/go-shellquote"
)

type GreenplumEnv interface {
	BinDir() string
	MasterDataDirectory() string
	MasterPort() int

	Run(utilityName string, arguments ...string) error
}

func (e *greenplumEnv) BinDir() string {
	return e.binDir
}

func (e *greenplumEnv) MasterDataDirectory() string {
	return e.masterDataDirectory
}

func (e *greenplumEnv) MasterPort() int {
	return e.masterPort
}

func (e *greenplumEnv) Run(utilityName string, arguments ...string) error {
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

type greenplumEnv struct {
	binDir              string
	masterDataDirectory string
	masterPort          int
}
