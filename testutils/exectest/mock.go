// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package exectest

import (
	"os/exec"

	"github.com/golang/mock/gomock"
)

// NewCommandMock returns an exectest.Command and a gomock object that controls
// its behavior. (The API is inspired by sqlmock, in which a database connection
// and a mock instance that controls its behavior are returned to the caller.)
//
// The returned mock may be used to register expected Command invocations, and
// optionally specify a Main invocation to run for any matching calls. (By
// default, any expected Commands will execute the exectest.Success
// implementation, which does nothing and exits with a zero status code.)
//
// For example:
//
//     command, mock := exectest.NewCommandMock(ctrl)
//
//     gomock.InOrder(
//         mock.EXPECT().Command("bash", "-c", "false || true"),
//         mock.EXPECT().Command("echo", "hello"),
//     )
//
// This code should be read, "We expect the command `bash -c 'false || true'` to
// be created, followed by the command `echo hello`."
//
//     command, mock := exectest.NewCommandMock(ctrl)
//
//     mock.EXPECT().Command("bash", gomock.Any()).
//         AnyTimes().
//         Return(exectest.Failure)
//     mock.EXPECT().Command("rsync", gomock.Any()).
//         AnyTimes()
//
// This code should be read, "We expect any number of commands invoking bash and
// rsync, with any arguments, in any order. All calls to bash should fail with
// status code 1."
//
// Due to a limitation in the exectest infrastructure, a mock expectation is met
// when the Command is constructed, NOT when it is run (via cmd.Run(),
// cmd.Output(), etc.). For this reason, using gomock.InOrder is only an
// approximation -- it's possible to construct two Commands in the correct order
// and run them in the opposite order, and the framework will not be able to
// detect it.
func NewCommandMock(ctrl *gomock.Controller) (command Command, mock *MockCommandSpy) {
	mock = NewMockCommandSpy(ctrl)

	command = func(executable string, args ...string) *exec.Cmd {
		main := mock.Command(executable, args...)
		if main == nil {
			main = Success
		}

		cmdf := NewCommand(main)
		return cmdf(executable, args...)
	}

	return command, mock
}

//go:generate ../../dev-bin/mockgen -destination spy.go -package exectest -self_package github.com/greenplum-db/gpupgrade/testutils/exectest github.com/greenplum-db/gpupgrade/testutils/exectest CommandSpy

// CommandSpy allows developers to register Command expectations during tests.
// It is the interface provided by the mock implementation returned by
// NewCommandMock; see the documentation for that function.
//
// You will not use this type directly; you will use it indirectly via gomock's
// generated MockCommandSpy.
type CommandSpy interface {
	Command(string, ...string) Main
}
