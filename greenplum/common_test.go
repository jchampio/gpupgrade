// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"github.com/greenplum-db/gpupgrade/testutils/exectest"
)

func init() {
	// Make sure all tests explicitly set execCommand.
	ResetExecCommand()
}

func SetExecCommand(cmdFunc exectest.Command) {
	execCommand = cmdFunc
}

func ResetExecCommand() {
	execCommand = nil
}
