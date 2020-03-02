package hub

import (
	"context"
	"sync"

	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
)

func UpdateRecoveryConfs(ctx context.Context, agentConns []*Connection, sourceCluster *utils.Cluster, targetCluster *utils.Cluster, initializeConfig InitializeConfig) error {

	var wg sync.WaitGroup
	errChan := make(chan error, len(agentConns))

	for _, conn := range agentConns {
		wg.Add(1)

		go func(conn *Connection) {
			defer wg.Done()

			// todo: Once we start saving the target config after adding mirrors, we should use those here instead.
			mirrors := utils.FilterSegmentsOnHost(initializeConfig.Mirrors, conn.Hostname)
			if len(mirrors) == 0 {
				return
			}

			var confs []*idl.RecoveryConfInfo
			for _, mirror := range mirrors {
				confs = append(confs, &idl.RecoveryConfInfo{
					TargetPrimaryPort:   int32(targetCluster.Primaries[mirror.ContentID].Port),
					SourcePrimaryPort:   int32(sourceCluster.Primaries[mirror.ContentID].Port),
					TargetMirrorDataDir: mirror.DataDir,
				})
			}

			_, err := conn.AgentClient.UpdateRecoveryConfs(ctx, &idl.UpdateRecoveryConfsRequest{RecoveryConfInfos: confs})
			if err != nil {
				errChan <- err
			}
		}(conn)
	}
	wg.Wait()
	close(errChan)

	var mErr error
	for err := range errChan {
		if err != nil {
			mErr = multierror.Append(mErr, err).ErrorOrNil()
		}
	}

	return mErr
}
