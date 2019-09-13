package commanders

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/greenplum-db/gpupgrade/idl"
)

type Preparer struct {
	client idl.CliToHubClient
}

func NewPreparer(client idl.CliToHubClient) Preparer {
	return Preparer{client: client}
}

var NumberOfConnectionAttempt = 100

func (p Preparer) ShutdownClusters() error {
	_, err := p.client.PrepareShutdownClusters(context.Background(),
		&idl.PrepareShutdownClustersRequest{})
	if err != nil {
		return err
	}

	fmt.Println("clusters shut down successfully")
	return nil
}

func (p Preparer) InitCluster() error {
	_, err := p.client.PrepareInitCluster(context.Background(), &idl.PrepareInitClusterRequest{})
	if err != nil {
		return err
	}

	fmt.Println("cluster successfully initialized")
	return nil
}

func (p Preparer) VerifyConnectivity(client idl.CliToHubClient) error {
	_, err := client.Ping(context.Background(), &idl.PingRequest{})
	for i := 0; i < NumberOfConnectionAttempt && err != nil; i++ {
		_, err = client.Ping(context.Background(), &idl.PingRequest{})
		time.Sleep(100 * time.Millisecond)
	}
	return err
}

func (p Preparer) TellHubToInitializeUpgrade(client idl.CliToHubClient, oldBinDir string, newBinDir string) error {
	_, err := client.TellHubToInitializeUpgrade(context.Background(), &idl.TellHubToInitializeUpgradeRequest{OldBinDir: oldBinDir, NewBinDir: newBinDir})
	if err != nil {
		return err
	}

	fmt.Println("hub started successfully")
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
