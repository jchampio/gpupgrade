package commanders

import (
	"context"
	"fmt"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpupgrade/idl"
)

// TODO: how should we find the gpupgrade_hub executable?  Right now, it's via newBinDir
func StartHub(newBinDir string) error {
	countHubs, err := HowManyHubsRunning()
	if err != nil {
		gplog.Error("failed to determine if hub already running")
		return err
	}
	if countHubs >= 1 {
		gplog.Error("gpupgrade_hub process already running")
		return errors.New("gpupgrade_hub process already running")
	}

	hub_path := path.Join(newBinDir, "gpupgrade_hub")
	cmd := exec.Command(hub_path, "--daemonize")
	stdout, cmdErr := cmd.Output()
	if cmdErr != nil {
		err := fmt.Errorf("failed to start hub (%s)", cmdErr)
		if exitErr, ok := cmdErr.(*exec.ExitError); ok {
			// Annotate with the Stderr capture, if we have it.
			err = fmt.Errorf("%s: %s", err, exitErr.Stderr)
		}
		return err
	}
	gplog.Debug("gpupgrade_hub started successfully: %s", stdout)
	return nil
}

func Initialize(client idl.CliToHubClient, oldBinDir, newBinDir string, oldPort int) (err error) {
	request := &idl.InitializeRequest{
		OldBinDir: oldBinDir,
		NewBinDir: newBinDir,
		OldPort:   int32(oldPort),
	}
	_, err = client.Initialize(context.Background(), request)
	if err != nil {
		return errors.Wrap(err, "initializing hub")
	}

	return nil
}

func HowManyHubsRunning() (int, error) {
	howToLookForHub := `ps -ef | grep -Gc "[g]pupgrade_hub$"` // use square brackets to avoid finding yourself in matches
	output, err := exec.Command("bash", "-c", howToLookForHub).Output()
	value, convErr := strconv.Atoi(strings.TrimSpace(string(output)))
	if convErr != nil {
		if err != nil {
			return -1, err
		}
		return -1, convErr
	}

	// let value == 0 through before checking err, for when grep finds nothing and its error-code is 1
	if value >= 0 {
		return value, nil
	}

	// only needed if the command errors, but somehow put a parsable & negative value on stdout
	return -1, err
}
