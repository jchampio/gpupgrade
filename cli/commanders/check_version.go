package commanders

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/greenplum-db/gpupgrade/idl"
)

type VersionChecker struct {
	client idl.CliToHubClient
}

func NewVersionChecker(client idl.CliToHubClient) VersionChecker {
	return VersionChecker{
		client: client,
	}
}

func (req VersionChecker) Execute() (err error) {
	description := "Checking version compatibility..."

	fmt.Printf("%s\r", FormatCustom(description, idl.StepStatus_RUNNING))
	defer func() {
		status := idl.StepStatus_COMPLETE
		if err != nil {
			status = idl.StepStatus_FAILED
		}

		fmt.Printf("%s\n", FormatCustom(description, status))
	}()

	resp, err := req.client.CheckVersion(context.Background(), &idl.CheckVersionRequest{})
	if err != nil {
		return errors.Wrap(err, "gRPC call to hub failed")
	}
	if !resp.IsVersionCompatible {
		return errors.New("Version Compatibility Check Failed")
	}

	return nil
}
