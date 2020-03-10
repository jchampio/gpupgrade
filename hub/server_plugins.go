// +build !debug

package hub

import "google.golang.org/grpc"

func RegisterPlugins(grpcServer *grpc.Server, s *Server) {
	// We have no plugins in the standard build.
}
