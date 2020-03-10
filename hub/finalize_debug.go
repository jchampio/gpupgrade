// +build debug

package hub

import (
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
)

func (s *Server) TestFinalizeIdempotence(_ *idl.FinalizeRequest, stream idl.DebugHub_TestFinalizeIdempotenceServer) error {
	runner := step.NewDoubleRunner(stream)

	s.RunFinalizeSubsteps(runner)

	return runner.Err
}
