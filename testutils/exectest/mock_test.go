// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package exectest_test

import (
	"errors"
	"fmt"
	"os/exec"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
)

const printedStdout = "stdout for PrintingMain"

func PrintingMain() {
	fmt.Print(printedStdout)
}

func init() {
	exectest.RegisterMains(PrintingMain)
}

func TestCommandMock(t *testing.T) {
	t.Run("records Command calls", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		command, mock := exectest.NewCommandMock(ctrl)

		gomock.InOrder(
			mock.EXPECT().Command("bash", "-c", "false || true"),
			mock.EXPECT().Command("echo", "hello"),
		)

		_ = command("bash", "-c", "false || true")
		_ = command("echo", "hello")
	})

	t.Run("can switch Main implementations based on expectations", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		command, mock := exectest.NewCommandMock(ctrl)

		gomock.InOrder(
			mock.EXPECT().Command("bash", gomock.Any()).
				Return(exectest.Failure),
			mock.EXPECT().Command("echo", gomock.Any()).
				Return(PrintingMain),
		)

		cmd := command("bash", "-c", "false || true")
		err := cmd.Run()

		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Errorf("got error %#v, want type %T", err, exitErr)
		} else if exitErr.ExitCode() != 1 {
			t.Errorf("mock bash call returned code %d, want %d", exitErr.ExitCode(), 1)
		}

		cmd = command("echo", "hello")
		out, err := cmd.Output()
		if err != nil {
			t.Errorf("running echo: %+v", err)
		}

		stdout := string(out)
		if stdout != printedStdout {
			t.Errorf("echo printed %q, want %q", out, printedStdout)
		}
	})
}
