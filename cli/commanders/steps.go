package commanders

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/idl"
)

type receiver interface {
	Recv() (*idl.Message, error)
}

var lines = map[idl.UpgradeSteps]string{
	idl.UpgradeSteps_CONFIG:                 "Retrieving configs...",
	idl.UpgradeSteps_START_AGENTS:           "Starting agents...",
	idl.UpgradeSteps_INIT_CLUSTER:           "Creating new cluster...",
	idl.UpgradeSteps_SHUTDOWN_CLUSTERS:      "Stopping clusters...",
	idl.UpgradeSteps_CONVERT_MASTER:         "Upgrading master...",
	idl.UpgradeSteps_COPY_MASTER:            "Copying master to segments...",
	idl.UpgradeSteps_CONVERT_PRIMARIES:      "Upgrading segments...",
	idl.UpgradeSteps_VALIDATE_START_CLUSTER: "Starting upgraded cluster...",
	idl.UpgradeSteps_RECONFIGURE_PORTS:      "Changing cluster ports...",
}

var indicators = map[idl.StepStatus]string{
	idl.StepStatus_RUNNING:  "[IN PROGRESS]",
	idl.StepStatus_COMPLETE: "[COMPLETE]",
	idl.StepStatus_FAILED:   "[FAILED]",
}

func Initialize(client idl.CliToHubClient, oldBinDir, newBinDir string, oldPort int) (err error) {
	request := &idl.InitializeRequest{
		OldBinDir: oldBinDir,
		NewBinDir: newBinDir,
		OldPort:   int32(oldPort),
	}

	stream, err := client.Initialize(context.Background(), request)
	if err != nil {
		return errors.Wrap(err, "initializing hub")
	}

	err = UILoop(stream, false)
	if err != nil {
		return xerrors.Errorf("Initialize: %w", err)
	}

	return nil
}

func Execute(client idl.CliToHubClient, verbose bool) error {
	fmt.Println()
	fmt.Println("Execute in progress.")
	fmt.Println()

	stream, err := client.Execute(context.Background(), &idl.ExecuteRequest{})
	if err != nil {
		// TODO: Change the logging message?
		gplog.Error("ERROR - Unable to connect to hub")
		return err
	}

	err = UILoop(stream, verbose)
	if err != nil {
		return xerrors.Errorf("Execute: %w", err)
	}

	fmt.Println(`
You may now run queries against the new database and perform any other
validation desired prior to finalizing your upgrade.

WARNING: If any queries modify the database during this time, this will affect
your revert time.

If you are satisfied with the state of the cluster, run "gpupgrade finalize" on
the command line to finish the upgrade.

If you would like to return the cluster to its original state, run
"gpupgrade revert" on the command line.`)

	return nil
}

func Finalize(client idl.CliToHubClient) error {
	fmt.Println()
	fmt.Println("Finalize in progress.")
	fmt.Println()

	stream, err := client.Finalize(context.Background(), &idl.FinalizeRequest{})
	if err != nil {
		gplog.Error(err.Error())
		return err
	}

	err = UILoop(stream, false)
	if err != nil {
		return xerrors.Errorf("Finalize: %w", err)
	}

	// TODO version number
	fmt.Println(`
The cluster is now upgraded and is ready to be used.`)

	return nil
}

func UILoop(stream receiver, verbose bool) error {
	var lastStep idl.UpgradeSteps
	var err error

	for {
		var msg *idl.Message
		msg, err = stream.Recv()
		if err != nil {
			break
		}

		switch x := msg.Contents.(type) {
		case *idl.Message_Chunk:
			if !verbose {
				continue
			}

			if x.Chunk.Type == idl.Chunk_STDOUT {
				os.Stdout.Write(x.Chunk.Buffer)
			} else if x.Chunk.Type == idl.Chunk_STDERR {
				os.Stderr.Write(x.Chunk.Buffer)
			}

		case *idl.Message_Status:
			// Rewrite the current line whenever we get an update for the
			// current step. (This behavior is switched off in verbose mode,
			// because it interferes with the output stream.)
			if !verbose {
				if lastStep == idl.UpgradeSteps_UNKNOWN_STEP {
					// This is the first call, so we don't need to "terminate"
					// the previous line at all.
				} else if x.Status.Step == lastStep {
					fmt.Print("\r")
				} else {
					fmt.Println()
				}
			}
			lastStep = x.Status.Step

			fmt.Printf(FormatStatus(x.Status))
			if verbose {
				fmt.Println()
			}

		default:
			panic(fmt.Sprintf("Unknown message type for Execute: %T", x))
		}
	}

	if !verbose {
		fmt.Println()
	}

	if err != io.EOF {
		return err
	}

	return nil
}

const format = "%-67s%-13s" // 80-column output; used for FormatXxx()

// FormatStatus returns a status string based on the upgrade status message.
// It's exported for ease of testing.
//
// FormatStatus panics if it doesn't have a string representation for a given
// protobuf code.
func FormatStatus(status *idl.UpgradeStepStatus) string {
	line, ok := lines[status.Step]
	if !ok {
		panic(fmt.Sprintf("unexpected step %#v", status.Step))
	}

	indicator, ok := indicators[status.Status]
	if !ok {
		panic(fmt.Sprintf("unexpected status %#v", status.Status))
	}

	return fmt.Sprintf(format, line, indicator)
}

// FormatCustom is like FormatStatus but accepts an arbitrary string for the
// step description.
func FormatCustom(description string, status idl.StepStatus) string {
	indicator, ok := indicators[status]
	if !ok {
		panic(fmt.Sprintf("unexpected status %#v", status))
	}

	return fmt.Sprintf(format, description, indicator)
}
