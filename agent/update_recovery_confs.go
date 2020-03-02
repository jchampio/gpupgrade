package agent

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"

	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
)

var sedCommand = exec.Command

func (s *Server) UpdateRecoveryConfs(ctx context.Context, request *idl.UpdateRecoveryConfsRequest) (*idl.UpdateRecoveryConfsReply, error) {
	var mErr error
	for _, conf := range request.RecoveryConfInfos {
		substitutionString := fmt.Sprintf("s/port=%d/port=%d/", conf.TargetPrimaryPort, conf.SourcePrimaryPort)
		path := filepath.Join(conf.TargetMirrorDataDir, "recovery.conf")

		err := sedCommand("sed", "-i.bak", substitutionString, path).Run()
		if err != nil {
			mErr = multierror.Append(mErr, err).ErrorOrNil()
		}
	}

	return &idl.UpdateRecoveryConfsReply{}, mErr
}
