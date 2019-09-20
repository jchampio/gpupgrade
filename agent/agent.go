package agent

import (
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/spf13/cobra"

	"github.com/greenplum-db/gpupgrade/agent/services"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/greenplum-db/gpupgrade/utils/daemon"
	"github.com/greenplum-db/gpupgrade/utils/log"
)

func Command() *cobra.Command {
	//debug.SetTraceback("all")
	//parser := flags.NewParser(&AllServices, flags.HelpFlag|flags.PrintErrors)
	//
	//_, err := parser.Parse()
	//if err != nil {
	//	os.Exit(utils.GetExitCodeForError(err))
	//}
	var logdir, statedir string
	var shouldDaemonize bool

	var cmd = &cobra.Command{
		Use:    "agent",
		Hidden: true,
		Short:  "Start the Command Listener (blocks)",
		Long:   `Start the Command Listener (blocks)`,
		Args:   cobra.MaximumNArgs(0), //no positional args allowed
		RunE: func(cmd *cobra.Command, args []string) error {
			gplog.SetLogger(nil)
			gplog.InitializeLogging("gpupgrade_agent", logdir)
			defer log.WritePanics()

			conf := services.AgentConfig{
				Port:     6416,
				StateDir: statedir,
			}

			agentServer := services.NewAgentServer(&cluster.GPDBExecutor{}, conf)
			if shouldDaemonize {
				agentServer.MakeDaemon()
			}

			agentServer.Start()

			agentServer.Stop()

			return nil
		},
	}

	cmd.Flags().StringVar(&logdir, "log-directory", "", "command_listener log directory")
	cmd.Flags().StringVar(&statedir, "state-directory", utils.GetStateDir(), "Agent state directory")

	daemon.MakeDaemonizable(cmd, &shouldDaemonize)

	return cmd
}
