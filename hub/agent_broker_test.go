package hub

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/greenplum-db/gpupgrade/idl/mock_idl"

	"github.com/greenplum-db/gpupgrade/idl"
)

func TestAgentBroker(t *testing.T) {
	t.Run("it sends a ReconfigureDataDirRequest with the given pairs to the agent client for the given hostname", func(t *testing.T) {
		// setup mock agent
		ctrl := gomock.NewController(t)
		client := mock_idl.NewMockAgentClient(ctrl)
		otherClient := mock_idl.NewMockAgentClient(ctrl)
		defer ctrl.Finish()

		// setup object under test
		// - give it several clients to ensure the the correct one is chosen
		broker := AgentBrokerGRPC{
			agentConnections: []*Connection{
				{
					AgentClient: otherClient,
					Hostname:    "myotherhost",
				},
				{
					AgentClient: client,
					Hostname:    "myhost",
				},
			},
			context: context.TODO(),
		}

		renamePairs := []*idl.RenamePair{
			{
				Src: "/some/source",
				Dst: "/some/destination",
			},
		}

		// setup expectation
		client.EXPECT().
			ReconfigureDataDirectories(broker.context, &idl.ReconfigureDataDirRequest{
				Pairs: renamePairs,
			}).
			Times(1)

		// Let's see what happens
		broker.ReconfigureDataDirectories("myhost", renamePairs)
	})

	t.Run("it returns an error if the hostname does not have corresponding agent client", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := mock_idl.NewMockAgentClient(ctrl)

		connections := []*Connection{
			{AgentClient: client, Hostname: "myhost"},
		}

		broker := AgentBrokerGRPC{
			agentConnections: connections,
			context:          context.TODO(),
		}

		renamePairs := []*idl.RenamePair{}

		err := broker.ReconfigureDataDirectories("other-host", renamePairs)

		if err == nil {
			t.Errorf("got no errors for ReconfigureDataDirectories, expected to not find an agent client for the hostname %v",
				"other-host")
		}

		ctrl.Finish()
	})
}
