package hub

import (
	"github.com/greenplum-db/gpupgrade/idl"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"golang.org/x/net/context"
)

func (h *Hub) SetConfig(ctx context.Context, in *idl.SetConfigRequest) (*idl.SetConfigReply, error) {
	switch in.Name {
	case "old-bindir":
		h.source.BinDir = in.Value
	case "new-bindir":
		h.target.BinDir = in.Value
	default:
		return nil, status.Errorf(codes.NotFound, "%s is not a valid configuration key", in.Name)
	}

	if err := h.SaveConfig(); err != nil {
		return &idl.SetConfigReply{}, err
	}

	gplog.Info("Successfully set %s to %s", in.Name, in.Value)
	return &idl.SetConfigReply{}, nil
}

func (h *Hub) GetConfig(ctx context.Context, in *idl.GetConfigRequest) (*idl.GetConfigReply, error) {
	resp := &idl.GetConfigReply{}

	switch in.Name {
	case "old-bindir":
		resp.Value = h.source.BinDir
	case "new-bindir":
		resp.Value = h.target.BinDir
	default:
		return nil, status.Errorf(codes.NotFound, "%s is not a valid configuration key", in.Name)
	}

	return resp, nil
}
