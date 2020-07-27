// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package exectest_test

import (
	"testing"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
)

func TestUnsetCommandPanicsWhenInvoked(t *testing.T) {
	execCommand := exectest.UnsetCommand()

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("did not panic")
		}
	}()

	execCommand("bash", "-c", "echo hello")
}
