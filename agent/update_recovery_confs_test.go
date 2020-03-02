package agent_test

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/agent"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/testutils/exectest"
)

func sedMain() {}

const failedCode = 42

func sedFailed() {
	os.Exit(failedCode)
}

func init() {
	exectest.RegisterMains(
		sedFailed,
		sedMain,
	)
}

func TestUpdateRecoveryConfPorts(t *testing.T) {
	agent.SetSedCommand(nil)
	server := agent.NewServer(agent.Config{
		Port:     -1,
		StateDir: "",
	})

	defer func() {
		agent.SetSedCommand(exec.Command)
	}()

	t.Run("it replaces the temporary port with the source port", func(t *testing.T) {
		called := false
		expectedArgs := [][]string{
			{
				"-i.bak", "s/port=1234/port=8000/", "/tmp/datadirs/mirror1_upgrade/gpseg0/recovery.conf",
			},
			{
				"-i.bak", "s/port=1235/port=8001/", "/tmp/datadirs/mirror2_upgrade/gpseg1/recovery.conf",
			},
		}
		calls := 0

		cmd := exectest.NewCommandWithVerifier(sedMain, func(path string, args ...string) {
			called = true

			if path != "sed" {
				t.Errorf(`got: %q want "sed"`, path)
			}

			expected := expectedArgs[calls]
			if !reflect.DeepEqual(args, expected) {
				t.Errorf("got args %#v want %#v", args, expected)
			}
			calls++
		})

		agent.SetSedCommand(cmd)

		confs := &idl.UpdateRecoveryConfsRequest{RecoveryConfInfos: []*idl.RecoveryConfInfo{
			{TargetPrimaryPort: 1234, SourcePrimaryPort: 8000, TargetMirrorDataDir: "/tmp/datadirs/mirror1_upgrade/gpseg0"},
			{TargetPrimaryPort: 1235, SourcePrimaryPort: 8001, TargetMirrorDataDir: "/tmp/datadirs/mirror2_upgrade/gpseg1"},
		}}

		_, err := server.UpdateRecoveryConfs(context.Background(), confs)

		if err != nil {
			t.Errorf("got error %+v want no error", err)
		}

		if !called {
			t.Errorf("Expected sedCommand to be called, but it was not")
		}

		if calls != len(expectedArgs) {
			t.Errorf("got %d calls want %d calls", calls, len(expectedArgs))
		}
	})

	t.Run("it returns all sed errors that occur", func(t *testing.T) {
		agent.SetSedCommand(exectest.NewCommand(sedFailed))
		confs := &idl.UpdateRecoveryConfsRequest{RecoveryConfInfos: []*idl.RecoveryConfInfo{
			{TargetPrimaryPort: 1234, SourcePrimaryPort: 8000, TargetMirrorDataDir: "/tmp/datadirs/mirror1_upgrade/gpseg0"},
			{TargetPrimaryPort: 1234, SourcePrimaryPort: 8000, TargetMirrorDataDir: "/tmp/datadirs/mirror1_upgrade/gpseg0"},
		}}

		_, err := server.UpdateRecoveryConfs(context.Background(), confs)

		var multiErr *multierror.Error
		if !xerrors.As(err, &multiErr) {
			t.Fatalf("got error %#v, want type %T", err, multiErr)
		}

		if len(multiErr.Errors) != 2 {
			t.Errorf("received %d errors, want %d", len(multiErr.Errors), 2)
		}

		var expected *exec.ExitError
		for _, err := range multiErr.Errors {
			if !xerrors.As(err, &expected) {
				t.Errorf("wanted error %+v got %+v", expected, err)
			}

			exitErr, ok := err.(*exec.ExitError)
			if !ok {
				t.Fatalf("unexpected error %#v", err)
			}

			if exitErr.ExitCode() != failedCode {
				t.Errorf("exit code %d want %d", exitErr.ExitCode(), failedCode)
			}
		}
	})
}
