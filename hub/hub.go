package hub

import (
	"fmt"
	"os"
	"runtime/debug"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/greenplum-db/gpupgrade/hub/services"
	"github.com/greenplum-db/gpupgrade/hub/upgradestatus"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/greenplum-db/gpupgrade/utils/daemon"
	"github.com/greenplum-db/gpupgrade/utils/log"
)

// This directory to have the implementation code for the gRPC server to serve
// Minimal CLI command parsing to embrace that booting this binary to run the hub might have some flags like a log dir

func Command() *cobra.Command {
	var logdir string
	var shouldDaemonize bool

	var cmd = &cobra.Command{
		Use:   "hub",
		Short: "Start the gpupgrade_hub (blocks)",
		Long:  `Start the gpupgrade_hub (blocks)`,
		Args:  cobra.MaximumNArgs(0), //no positional args allowed
		RunE: func(cmd *cobra.Command, args []string) error {
			gplog.SetLogger(nil)
			gplog.InitializeLogging("gpupgrade_hub", logdir)
			debug.SetTraceback("all")
			defer log.WritePanics()

			conf := &services.HubConfig{
				CliToHubPort:   7527,
				HubToAgentPort: 6416,
				StateDir:       utils.GetStateDir(),
				LogDir:         logdir,
			}
			err := os.Mkdir(conf.StateDir, 0700)
			if os.IsExist(err) {
				return fmt.Errorf("gpupgrade state dir (%s) already exists. Did you already run gpupgrade initialize?", conf.StateDir)
			} else if err != nil {
				return err
			}
			cm := upgradestatus.NewChecklistManager(conf.StateDir)

			hub := services.NewHub(nil, nil, grpc.DialContext, conf, cm)

			// Set up the checklist steps in order.
			//
			// TODO: make sure the implementations here, and the Checklist below, are
			// fully exercised in end-to-end tests. It feels like we should be able to
			// pull these into a Hub method or helper function, but currently the
			// interfaces aren't well componentized.
			cm.AddWritableStep(upgradestatus.CONFIG, idl.UpgradeSteps_CONFIG)
			cm.AddWritableStep(upgradestatus.START_AGENTS, idl.UpgradeSteps_START_AGENTS)
			cm.AddWritableStep(upgradestatus.INIT_CLUSTER, idl.UpgradeSteps_INIT_CLUSTER)
			cm.AddWritableStep(upgradestatus.SHUTDOWN_CLUSTERS, idl.UpgradeSteps_SHUTDOWN_CLUSTERS)
			cm.AddWritableStep(upgradestatus.CONVERT_MASTER, idl.UpgradeSteps_CONVERT_MASTER)
			cm.AddWritableStep(upgradestatus.COPY_MASTER, idl.UpgradeSteps_COPY_MASTER)

			cm.AddReadOnlyStep(upgradestatus.CONVERT_PRIMARIES, idl.UpgradeSteps_CONVERT_PRIMARIES,
				func(stepName string) idl.StepStatus {
					return services.PrimaryConversionStatus(hub)
				})

			cm.AddWritableStep(upgradestatus.VALIDATE_START_CLUSTER, idl.UpgradeSteps_VALIDATE_START_CLUSTER)
			cm.AddWritableStep(upgradestatus.RECONFIGURE_PORTS, idl.UpgradeSteps_RECONFIGURE_PORTS)

			if shouldDaemonize {
				hub.MakeDaemon()
			}

			err = hub.Start()
			if err != nil {
				return err
			}

			return nil
		},
	}

	cmd.PersistentFlags().StringVar(&logdir, "log-directory", "", "gpupgrade_hub log directory")

	daemon.MakeDaemonizable(cmd, &shouldDaemonize)

	return cmd
}
