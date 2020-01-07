package commanders

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/greenplum-db/gpupgrade/hub"
	"github.com/greenplum-db/gpupgrade/utils"
)

// introduce this variable to allow exec.Command to be mocked out in tests
var execCommandHubStart = exec.Command
var execCommandHubCount = exec.Command

// we create the state directory in the cli to ensure that at most one gpupgrade is occuring
// at the same time.
func CreateStateDirAndClusterConfigs(sourceBinDir, targetBinDir string) (err error) {
	s := Substep("Creating directories...")
	defer s.Finish(&err)

	stateDir := utils.GetStateDir()
	err = os.Mkdir(stateDir, 0700)
	if os.IsExist(err) {
		return fmt.Errorf("gpupgrade state dir (%s) already exists. Did you already run gpupgrade initialize?", stateDir)
	} else if err != nil {
		return err
	}

	// Create an empty JSON configuration for the hub.
	file, err := os.Create(filepath.Join(stateDir, hub.ConfigFileName))
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintf(file, "{}") // the hub will fill this in during initialization
	return nil
}

func StartHub() (err error) {
	s := Substep("Starting hub...")
	defer s.Finish(&err)

	cmd := execCommandHubStart("gpupgrade", "hub", "--daemonize")
	stdout, cmdErr := cmd.Output()
	if cmdErr != nil {
		err := fmt.Errorf("failed to start hub (%s)", cmdErr)
		if exitErr, ok := cmdErr.(*exec.ExitError); ok {
			// Annotate with the Stderr capture, if we have it.
			err = fmt.Errorf("%s: %s", err, exitErr.Stderr)
		}
		return err
	}
	gplog.Debug("gpupgrade hub started successfully: %s", stdout)
	return nil
}

func IsHubRunning() (bool, error) {
	script := `ps -ef | grep -wGc "[g]pupgrade hub"` // use square brackets to avoid finding yourself in matches
	_, err := execCommandHubCount("bash", "-c", script).Output()

	if exitError, ok := err.(*exec.ExitError); ok {
		if exitError.ProcessState.ExitCode() == 1 { // hub not found
			return false, nil
		}
	}
	if err != nil { // grep failed
		return false, err
	}

	return true, nil
}
