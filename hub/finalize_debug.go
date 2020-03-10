// +build debug

package hub

import (
	multierror "github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
)

func (s *Server) TestFinalizeIdempotence(_ *idl.FinalizeRequest, stream idl.DebugHub_TestFinalizeIdempotenceServer) error {
	runner := step.NewDoubleRunner(stream)

	s.RunFinalizeSubsteps(runner)

	var err error

	for _, derr := range runner.DoubleErrors {
		err = multierror.Append(err, derr).ErrorOrNil()
	}

	if runner.Err != nil {
		err = multierror.Append(err, runner.Err).ErrorOrNil()
	}

	return err
}
