// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"

	"github.com/hashicorp/go-multierror"

	. "github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/upgrade"
)

var RestorePgControl = upgrade.RestorePgControl

func (s *Server) RestorePrimariesPgControl(ctx context.Context, in *RestorePgControlRequest) (*RestorePgControlReply, error) {
	var mErr *multierror.Error
	for _, dir := range in.Datadirs {
		err := RestorePgControl(dir)
		if err != nil {
			mErr = multierror.Append(mErr, err)
		}
	}

	return &RestorePgControlReply{}, mErr.ErrorOrNil()
}
