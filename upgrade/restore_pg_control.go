// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package upgrade

import (
	"os"
	"path/filepath"

	"github.com/greenplum-db/gp-common-go-libs/gplog"

	"github.com/greenplum-db/gpupgrade/utils"
)

// RestorePgControl restores pg_control.old to pg_control,
// if pg_control.old is not existing, then pg_control
// must exist, otherwise an error is thrown.
func RestorePgControl(dir string) error {
	globalDir := filepath.Join(dir, "global")
	src := filepath.Join(globalDir, "pg_control.old")
	dst := filepath.Join(globalDir, "pg_control")

	_, err := utils.System.Stat(src)
	if os.IsNotExist(err) {
		gplog.Debug("file %s does not exist", src)
		_, err = utils.System.Stat(dst)
		if err != nil {
			return err
		}

		gplog.Debug("file %s exists", dst)
		return nil
	} else if err != nil {
		return err
	}

	gplog.Debug("renaming %s to %s", src, dst)
	return utils.System.Rename(src, dst)
}
