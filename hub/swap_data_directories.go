package hub

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

func SwapDataDirectories(hub Hub, agentBroker AgentBroker) error {
	swapper := finalizer{agentBroker: agentBroker}
	swapper.archive(hub.masterPair.source)
	swapper.publish(hub.masterPair.target, hub.masterPair.source)
	swapper.swapDirectoriesOnAgents(hub.agents)
	return swapper.Errors()
}

type finalizer struct {
	err         *multierror.Error
	agentBroker AgentBroker
}

func (f *finalizer) archive(sourceSegment utils.SegConfig) {
	err := renameDirectory(sourceSegment.DataDir, sourceSegment.ArchivingDataDirectory())
	f.err = multierror.Append(f.err, err)
}

func (f *finalizer) publish(targetSegment utils.SegConfig, sourceSegment utils.SegConfig) {
	err := renameDirectory(targetSegment.DataDir, targetSegment.PublishingDataDirectory(sourceSegment))
	f.err = multierror.Append(f.err, err)
}

func (f *finalizer) swapDirectoriesOnAgents(agents []Agent) {
	result := make(chan error, len(agents))

	for _, agent := range agents {
		agent := agent // capture agent variable

		go func() {
			result <- f.agentBroker.ReconfigureDataDirectories(agent.hostname,
				makeRenamePairs(agent.segmentPairs))
		}()
	}

	for range agents {
		multierror.Append(f.err, <-result)
	}
}

func (f *finalizer) Errors() error {
	return f.err.ErrorOrNil()
}

func makeRenamePairs(pairs []SegmentPair) []*idl.RenamePair {
	var renamePairs []*idl.RenamePair

	for _, pair := range pairs {
		// Archive source
		renamePairs = append(renamePairs, &idl.RenamePair{
			Src: pair.source.DataDir,
			Dst: pair.source.ArchivingDataDirectory(),
		})

		// Publish target
		renamePairs = append(renamePairs, &idl.RenamePair{
			Src: pair.target.DataDir,
			Dst: pair.target.PublishingDataDirectory(pair.source),
		})
	}

	return renamePairs
}

func renameDirectory(originalName, newName string) error {
	gplog.Info(fmt.Sprintf("moving directory %v to %v", originalName, newName))

	return utils.System.Rename(originalName, newName)
}
