package hub

import (
	"fmt"
	"os/exec"
	"path/filepath"
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
	commandAsString := exec.Command(
		filepath.Join(e.binDir, utilityName), arguments...,
	).String()

	withGreenplumPath := fmt.Sprintf("source %s/../greenplum_path.sh && %s", e.binDir, commandAsString)

	command := exec.Command("bash", "-c", withGreenplumPath)
	command.Env = append(command.Env, fmt.Sprintf("%v=%v", "MASTER_DATA_DIRECTORY", e.masterDataDirectory))
	command.Env = append(command.Env, fmt.Sprintf("%v=%v", "PGPORT", e.masterPort))
	output, err := command.CombinedOutput()

	fmt.Printf("Master data directory, %v\n", e.masterDataDirectory)
	fmt.Printf("%s: %s \n", command.String(), string(output))

	return err
}

type greenplumEnv struct {
	binDir              string
	masterDataDirectory string
	masterPort          int
}

func NewGreenplumEnv(binDir string, masterDataDirectory string) *greenplumEnv {
	if binDir == "" {
		panic(fmt.Sprintf("invalid bin dir: %v", binDir))
	}

	return &greenplumEnv{
		binDir:              binDir,
		masterDataDirectory: masterDataDirectory,
	}
}
