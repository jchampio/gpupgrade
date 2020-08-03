// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import "os/exec"

// execCommand should be used instead of exec.Command in all package code, so
// that the test framework can intercept external command invocations.
var execCommand = exec.Command
