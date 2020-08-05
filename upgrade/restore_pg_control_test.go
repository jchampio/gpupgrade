// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package upgrade_test

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"

	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/upgrade"
	"github.com/greenplum-db/gpupgrade/utils"
)

// Common setup required for multiple test cases
func setupGlobalDir(t *testing.T) string {
	source := testutils.GetTempDir(t, "")

	globalDir := filepath.Join(source, "global")
	err := utils.System.Mkdir(globalDir, 0755)
	if err != nil {
		t.Fatalf("failed to create dir %s", globalDir)
	}

	return source
}

func TestRestorePgControl(t *testing.T) {
	testhelper.SetupTestLogger()

	t.Run("restores pg_control successfully", func(t *testing.T) {
		source := setupGlobalDir(t)
		defer testutils.MustRemoveAll(t, source)

		pgControlFile := filepath.Join(source, "global", "pg_control.old")
		err := ioutil.WriteFile(pgControlFile, []byte{}, 0644)
		if err != nil {
			t.Fatalf("failed to write file %s, %#v", pgControlFile, err)
		}

		err = upgrade.RestorePgControl(source)
		if err != nil {
			t.Errorf("unexpected error %#v", err)
		}

		targetFile := filepath.Join(source, "global", "pg_control")
		_, err = os.Stat(targetFile)
		if err != nil {
			t.Errorf("expected file %s to exist, got error %#v", targetFile, err)
		}
	})

	t.Run("re-run of RestorePgControl finishes successfully if RestorePgControl already succeeded before", func(t *testing.T) {
		source := setupGlobalDir(t)
		defer testutils.MustRemoveAll(t, source)

		pgControlFile := filepath.Join(source, "global", "pg_control")
		err := ioutil.WriteFile(pgControlFile, []byte{}, 0644)
		if err != nil {
			t.Fatalf("failed to write file %s, %#v", pgControlFile, err)
		}

		err = upgrade.RestorePgControl(source)
		if err != nil {
			t.Errorf("unexpected error %#v", err)
		}
	})

	t.Run("should fail when pg_control.old and pg_control does not exist", func(t *testing.T) {
		source := setupGlobalDir(t)
		defer testutils.MustRemoveAll(t, source)

		err := upgrade.RestorePgControl(source)
		if err == nil {
			t.Errorf("expected error")
		}
	})

	t.Run("should fail if src file exist but stat resulted in an error", func(t *testing.T) {
		source := setupGlobalDir(t)
		defer testutils.MustRemoveAll(t, source)

		pgControlFile := filepath.Join(source, "global", "pg_control.old")
		utils.System.Stat = func(path string) (os.FileInfo, error) {
			if path != pgControlFile {
				t.Errorf("got path %q, want %q", path, pgControlFile)
			}

			return nil, errors.New("permission denied")
		}

		err := upgrade.RestorePgControl(source)
		if err == nil {
			t.Errorf("expected error")
		}
	})
}
