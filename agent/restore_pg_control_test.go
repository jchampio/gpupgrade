// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package agent_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	"github.com/hashicorp/go-multierror"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/agent"
	"github.com/greenplum-db/gpupgrade/idl"
)

func TestServer_RestorePrimariesPgControl(t *testing.T) {
	testhelper.SetupTestLogger()
	server := agent.NewServer(agent.Config{})

	t.Run("bubbles up errors", func(t *testing.T) {
		dirs := []string{"/tmp/test1", "/tmp/test2"}
		_, err := server.RestorePrimariesPgControl(context.Background(), &idl.RestorePgControlRequest{Datadirs: dirs})
		var mErr *multierror.Error
		if !xerrors.As(err, &mErr) {
			t.Errorf("error %#v does not contain type %T", err, mErr)
			return
		}

		if len(dirs) != mErr.Len() {
			t.Errorf("got error count %d, want %d", mErr.Len(), len(dirs))
		}

		for i, err := range mErr.Errors {
			if !os.IsNotExist(err) {
				t.Errorf("got error type %T, want %T", err, &os.PathError{})
			}

			if !strings.Contains(err.(*os.PathError).Path, dirs[i]) {
				t.Errorf("got path %s, want %s", err.(*os.PathError).Path, dirs[i])
			}
		}
	})

	t.Run("finishes successfully", func(t *testing.T) {
		agent.RestorePgControl = func(dir string) error {
			return nil
		}

		_, err := server.RestorePrimariesPgControl(context.Background(), &idl.RestorePgControlRequest{Datadirs: []string{}})
		if err != nil {
			t.Errorf("unexpected error %#v", err)
		}
	})
}
