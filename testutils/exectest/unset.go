// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package exectest

import "os/exec"

// UnsetCommand is a setup helper for packages that use exectest.NewCommand.
// Setting the Command entry point to nil is safe, but leads to confusing
// crashes when tests forget to set the entry point to something else. Setting
// the entry point to an UnsetCommand() will remind developers in a more
// friendly way.
func UnsetCommand() Command {
	return func(_ string, _ ...string) *exec.Cmd {
		panic("the current test has forgotten to set up an exectest.NewCommand()")
	}
}
