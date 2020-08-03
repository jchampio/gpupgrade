// Copyright (c) 2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/blang/semver"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
)

func PostgresGPVersion_5_27_0_beta() {
	fmt.Println("postgres (Greenplum Database) 5.27.0+beta.4 build commit:baef9b9ba885f2f4e4a87d5e201caae969ef4401")
}

func PostgresGPVersion_6_dev() {
	fmt.Println("postgres (Greenplum Database) 6.0.0-beta.1 build dev")
}

func PostgresGPVersion_6_7_1() {
	fmt.Println("postgres (Greenplum Database) 6.7.1 build commit:a21de286045072d8d1df64fa48752b7dfac8c1b7")
}

func PostgresGPVersion_11_341_31() {
	fmt.Println("postgres (Greenplum Database) 11.341.31 build commit:a21de286045072d8d1df64fa48752b7dfac8c1b7")
}

func init() {
	exectest.RegisterMains(
		PostgresGPVersion_5_27_0_beta,
		PostgresGPVersion_6_dev,
		PostgresGPVersion_6_7_1,
		PostgresGPVersion_11_341_31,
	)
}

func TestGPHomeVersion(t *testing.T) {
	const gphome = "/usr/local/my-gpdb-home"
	postgresPath := filepath.Join(gphome, "bin", "postgres")

	// Common verifier for all tests' calls to postgres.
	verifier := func(t *testing.T) func(string, ...string) {
		return func(executable string, args ...string) {
			if executable != postgresPath {
				t.Errorf("called %q, want %q", executable, postgresPath)
			}

			expected := []string{"--gp-version"}
			if !reflect.DeepEqual(args, expected) {
				t.Errorf("args were %q, want %q", args, expected)
			}
		}
	}

	cases := []struct {
		name     string
		execMain exectest.Main // the postgres Main implementation to run
		expected string        // the expected semantic version; e.g. "5.1.14"
	}{
		{"handles development versions", PostgresGPVersion_6_dev, "6.0.0"},
		{"handles beta versions", PostgresGPVersion_5_27_0_beta, "5.27.0"},
		{"handles release versions", PostgresGPVersion_6_7_1, "6.7.1"},
		{"handles large versions", PostgresGPVersion_11_341_31, "11.341.31"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			execCmd := exectest.NewCommandWithVerifier(c.execMain, verifier(t))

			SetExecCommand(execCmd)
			defer ResetExecCommand()

			version, err := GPHomeVersion(gphome)
			if err != nil {
				t.Errorf("returned error: %+v", err)
			}

			expected := semver.MustParse(c.expected)
			if !version.Equals(expected) {
				t.Errorf("got version %v, want %v", version, expected)
			}
		})
	}

	formatErrorCases := []struct {
		name    string
		version string
	}{
		{"empty string", ""},
		{"only a marker", "(Greenplum Database)"},
	}

	for _, c := range formatErrorCases {
		t.Run(fmt.Sprintf("returns error with %s as input", c.name), func(t *testing.T) {
			_, err := parseGPVersion(c.version)
			if !errors.Is(err, ErrUnknownVersion) {
				t.Errorf("returned error %+v, want %+v", err, ErrUnknownVersion)
			}
		})
	}

	t.Run("bubbles up postgres execution failures", func(t *testing.T) {
		execCmd := exectest.NewCommandWithVerifier(exectest.Failure, verifier(t))

		SetExecCommand(execCmd)
		defer ResetExecCommand()

		_, err := GPHomeVersion(gphome)

		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Errorf("returned error %#v, want type %T", err, exitErr)
		}
	})
}
