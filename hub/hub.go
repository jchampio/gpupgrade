package hub

import "github.com/greenplum-db/gpupgrade/utils"

//
// Build a hub-centric model of the world:
//
// A hub has agents, agents have segment pairs
//
func MakeHub(config *Config) Hub {
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
		configs = append(configs, Agent{
			hostname:     hostname,
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
	hostname     string
	segmentPairs []SegmentPair
}

type SegmentPair struct {
	source utils.SegConfig
	target utils.SegConfig
}
