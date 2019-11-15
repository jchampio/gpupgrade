package exectest_test

import (
	"os/exec"
	"testing"

	"golang.org/x/xerrors"

	. "github.com/greenplum-db/gpupgrade/testutils/exectest"
)

func TestBuiltinMains(t *testing.T) {
	t.Run("Success()", func(t *testing.T) {
		success := NewCommand(Success)("/unused/path")
		err := success.Run()
		if err != nil {
			t.Errorf("exited with error %v", err)

			var exitErr *exec.ExitError
			if xerrors.As(err, &exitErr) {
				t.Logf("subprocess stderr follows:\n%s", string(exitErr.Stderr))
			}
		}
	})

	t.Run("Failure()", func(t *testing.T) {
		failure := NewCommand(Failure)("/unused/path")
		err := failure.Run()
		if err == nil {
			t.Fatal("exited without an error")
		}

		var exitErr *exec.ExitError
		if !xerrors.As(err, &exitErr) {
			t.Fatalf("unexpected error %#v", err)
		}
		if exitErr.ExitCode() != 1 {
			t.Errorf("exit code %d want %d", exitErr.ExitCode(), 1)
		}
	})
}
