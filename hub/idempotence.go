// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"fmt"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
)

// idempotenceRunner is a SubstepRunner that always fails the first run of each
// substep after it completes. It uses the wrapped Step for actual substep
// execution.
//
// The purpose of this implementation is to assist regression testing
// frameworks; it's not intended to be used in production.
type idempotenceRunner struct {
	st *step.Step // used to Run() substeps
}

func (i idempotenceRunner) Run(substep idl.Substep, f func(step.OutStreams) error) {
	i.st.Run(substep, i.wrap(substep, f))
}

func (i idempotenceRunner) AlwaysRun(substep idl.Substep, f func(step.OutStreams) error) {
	i.st.AlwaysRun(substep, i.wrap(substep, f))
}

func (i idempotenceRunner) wrap(substep idl.Substep, f func(step.OutStreams) error) func(step.OutStreams) error {
	alreadyRun, err := step.HasRun(i.st.Name(), substep)
	if err != nil {
		// This is a test-only implementation; no need to be clever.
		panic(fmt.Sprintf("couldn't retrieve substep status: %v", err))
	}

	if alreadyRun {
		// For subsequent runs, just run the substep as intended.
		return f
	}

	// For the first run, force a failure no matter what.
	return func(streams step.OutStreams) error {
		if err := f(streams); err != nil {
			return err
		}

		return DebugIdempotenceError{}
	}
}

// DebugIdempotenceError is a sentinel error injected by idempotenceRunner on
// the first run of any substep.
type DebugIdempotenceError struct{}

func (d DebugIdempotenceError) Error() string {
	return "forced error for idempotence testing"
}
