package hub

import (
	"context"

	multierror "github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
)

func SampleThing(conns []*Connection, ratio float64) error {
	req := &idl.CheckSegmentDiskSpaceRequest{
		Request: &idl.CheckDiskSpaceRequest{
			Ratio: ratio,
		},
		Datadirs: []string{},
	}
	errors := make(chan error, len(conns))

	for _, conn := range conns {
		conn := conn // capture the range variable

		go func() {
			_, err := conn.AgentClient.CheckDiskSpace(context.TODO(), req)
			errors <- err
		}()
	}

	var merr error
	for range conns {
		err := <-errors
		merr = multierror.Append(merr, err).ErrorOrNil()
	}

	return merr
}
