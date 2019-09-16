package commanders

import (
	"context"
	"fmt"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"
)

// FIXME: reconcile these with the ones in the `command` package

// connTimeout retrieves the GPUPGRADE_CONNECTION_TIMEOUT environment variable,
// interprets it as a (possibly fractional) number of seconds, and converts it
// into a Duration. The default is one second if the envvar is unset or
// unreadable.
//
// TODO: should we make this a global --option instead?
func connTimeout() time.Duration {
	const defaultDuration = time.Second

	seconds, ok := os.LookupEnv("GPUPGRADE_CONNECTION_TIMEOUT")
	if !ok {
		return defaultDuration
	}

	duration, err := strconv.ParseFloat(seconds, 64)
	if err != nil {
		gplog.Warn(`GPUPGRADE_CONNECTION_TIMEOUT of "%s" is invalid (%s); using default of one second`,
			seconds, err)
		return defaultDuration
	}

	return time.Duration(duration * float64(time.Second))
}

// connectToHub() performs a blocking connection to the hub, and returns a
// CliToHubClient which wraps the resulting gRPC channel. Any errors result in
// an os.Exit(1).
func connectToHub() idl.CliToHubClient {
	upgradePort := os.Getenv("GPUPGRADE_HUB_PORT")
	if upgradePort == "" {
		upgradePort = "7527"
	}

	hubAddr := "localhost:" + upgradePort

	// Set up our timeout.
	ctx, cancel := context.WithTimeout(context.Background(), connTimeout())
	defer cancel()

	// Attempt a connection.
	conn, err := grpc.DialContext(ctx, hubAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		// Print a nicer error message if we can't connect to the hub.
		if ctx.Err() == context.DeadlineExceeded {
			gplog.Error("couldn't connect to the upgrade hub (did you run 'gpupgrade prepare start-hub'?)")
		} else {
			gplog.Error(err.Error())
		}
		os.Exit(1)
	}

	return idl.NewCliToHubClient(conn)
}

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

func VerifyConnectivity(client idl.CliToHubClient) error {
	_, err := client.Ping(context.Background(), &idl.PingRequest{})
	for i := 0; i < NumberOfConnectionAttempt && err != nil; i++ {
		_, err = client.Ping(context.Background(), &idl.PingRequest{})
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

func TellHubToInitializeUpgrade(client idl.CliToHubClient, oldBinDir, newBinDir string, oldPort int) error {
	_, err := client.TellHubToInitializeUpgrade(context.Background(), &idl.TellHubToInitializeUpgradeRequest{OldBinDir: oldBinDir, NewBinDir: newBinDir, OldPort: int32(oldPort)})
	if err != nil {
		return err
	}

	fmt.Println("hub started successfully")
	return nil
}

func InitializeStep(oldBinDir, newBinDir string, oldPort int) error {

	err := StartHub(newBinDir)
	if err != nil {
		gplog.Error(err.Error())
		os.Exit(1)
	}

	client := connectToHub()

	err = VerifyConnectivity(client)
	if err != nil {
		gplog.Error("gpupgrade is unable to connect via gRPC to the hub")
		gplog.Error("%v", err)
		os.Exit(1)
	}

	err = TellHubToInitializeUpgrade(client, oldBinDir, newBinDir, oldPort)
	if err != nil {
		gplog.Error(err.Error())
		os.Exit(1)
	}

	return nil
}
