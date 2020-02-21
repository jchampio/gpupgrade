package hub

import (
	"fmt"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

//
// Build a hub-centric model of the world:
//
// A hub has agents, agents have segment pairs
//
func MakeHub(config *Config, clients map[string]idl.AgentClient) Hub {
	var segmentPairsByHost = make(map[string][]SegmentPair)

	for contentId, sourceSegment := range config.Source.Primaries {
		if contentId == -1 {
			continue
		}

		if segmentPairsByHost[sourceSegment.Hostname] == nil {
			segmentPairsByHost[sourceSegment.Hostname] = []SegmentPair{}
		}

		segmentPairsByHost[sourceSegment.Hostname] = append(segmentPairsByHost[sourceSegment.Hostname], SegmentPair{
			source: sourceSegment,
			target: config.Target.Primaries[contentId],
		})
	}

	var configs []Agent
	for hostname, agentSegmentPairs := range segmentPairsByHost {
		client, ok := clients[hostname]
		if !ok {
			panic(fmt.Sprintf("no connected client for host %q", hostname))
		}

		configs = append(configs, Agent{
			AgentClient:  client,
			segmentPairs: agentSegmentPairs,
		})
	}

	return Hub{
		masterPair: SegmentPair{
			source: config.Source.Primaries[-1],
			target: config.Target.Primaries[-1],
		},
		agents: configs,
	}
}

type Hub struct {
	masterPair SegmentPair
	agents     []Agent
}

type Agent struct {
	idl.AgentClient

	segmentPairs []SegmentPair
}

type SegmentPair struct {
	source utils.SegConfig
	target utils.SegConfig
}
