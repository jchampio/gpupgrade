package services

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	"github.com/greenplum-db/gpupgrade/idl"
)

func (h *Hub) Execute(request *idl.ExecuteRequest, stream idl.CliToHub_ExecuteServer) (err error) {
	substeps, err := h.BeginStep("execute", stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := substeps.Finish(); ferr != nil {
			err = multierror.Append(err, ferr).ErrorOrNil()
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("execute: %s", err))
		}
	}()

	substeps.Run(upgradestatus.UPGRADE_MASTER, func(streams OutStreams) error {
		return h.UpgradeMaster(streams, false)
	})
	substeps.Run(upgradestatus.COPY_MASTER,
		h.CopyMasterDataDir,
	)
	substeps.Run(upgradestatus.UPGRADE_PRIMARIES, func(_ OutStreams) error {
		return h.ConvertPrimaries(false)
	})
	substeps.Run(upgradestatus.START_TARGET_CLUSTER, func(streams OutStreams) error {
		return StartCluster(streams, h.target)
	})

	return substeps.Err()
}
