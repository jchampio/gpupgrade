// +build debug

package hub

import (
	"google.golang.org/grpc"

	"github.com/greenplum-db/gpupgrade/idl"
)

func RegisterPlugins(grpcServer *grpc.Server, s *Server) {
	idl.RegisterDebugHubServer(grpcServer, s)
}
