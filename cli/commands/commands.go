// Copyright (c) 2017-2020 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package commands

/*
 *  This file generates the command-line cli that is the heart of gpupgrade.  It uses Cobra to generate
 *    the cli based on commands and sub-commands. The below in this comment block shows a notional example
 *    of how this looks to give you an idea of what the command structure looks like at the cli.  It is NOT necessarily
 *    up-to-date but is a useful as an orientation to what is going on here.
 *
 * example> gpupgrade
 * 	   2018/09/28 16:09:39 Please specify one command of: check, config, prepare, status, upgrade, or version
 *
 * example> gpupgrade check
 *      collects information and validates the target Greenplum installation can be upgraded
 *
 *      Usage:
 * 		gpupgrade check [command]
 *
 * 		Available Commands:
 * 			config       gather cluster configuration
 * 			disk-space   check that disk space usage is less than 80% on all segments
 * 			object-count count database objects and numeric objects
 * 			version      validate current version is upgradable
 *
 * 		Flags:
 * 			-h, --help   help for check
 *
 * 		Use "gpupgrade check [command] --help" for more information about a command.
 */

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/greenplum-db/gpupgrade/cli"
	"github.com/greenplum-db/gpupgrade/cli/commanders"
	"github.com/greenplum-db/gpupgrade/hub"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
	"github.com/greenplum-db/gpupgrade/upgrade"
	"github.com/greenplum-db/gpupgrade/utils"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
	"github.com/greenplum-db/gpupgrade/utils/stopwatch"
)

var (
	Help           map[string]string
	InitializeHelp string
	ExecuteHelp    string
	FinalizeHelp   string
	RevertHelp     string
)

func init() {
	InitializeHelp = GenerateHelpString(initializeHelp, []idl.Substep{
		idl.Substep_CREATING_DIRECTORIES,
		idl.Substep_START_HUB,
		idl.Substep_SAVING_SOURCE_CLUSTER_CONFIG,
		idl.Substep_START_AGENTS,
		idl.Substep_CHECK_DISK_SPACE,
		idl.Substep_CREATE_TARGET_CONFIG,
		idl.Substep_INIT_TARGET_CLUSTER,
		idl.Substep_SHUTDOWN_TARGET_CLUSTER,
		idl.Substep_BACKUP_TARGET_MASTER,
		idl.Substep_CHECK_UPGRADE,
	})
	ExecuteHelp = GenerateHelpString(executeHelp, []idl.Substep{
		idl.Substep_SHUTDOWN_SOURCE_CLUSTER,
		idl.Substep_UPGRADE_MASTER,
		idl.Substep_COPY_MASTER,
		idl.Substep_UPGRADE_PRIMARIES,
		idl.Substep_START_TARGET_CLUSTER,
	})
	FinalizeHelp = GenerateHelpString(finalizeHelp, []idl.Substep{
		idl.Substep_SHUTDOWN_TARGET_CLUSTER,
		idl.Substep_UPDATE_TARGET_CATALOG_AND_CLUSTER_CONFIG,
		idl.Substep_UPDATE_DATA_DIRECTORIES,
		idl.Substep_UPDATE_TARGET_CONF_FILES,
		idl.Substep_START_TARGET_CLUSTER,
		idl.Substep_UPGRADE_STANDBY,
		idl.Substep_UPGRADE_MIRRORS,
	})
	RevertHelp = GenerateHelpString(revertHelp, []idl.Substep{
		idl.Substep_SHUTDOWN_TARGET_CLUSTER,
		idl.Substep_DELETE_TARGET_CLUSTER_DATADIRS,
		idl.Substep_DELETE_SEGMENT_STATEDIRS,
		idl.Substep_STOP_HUB_AND_AGENTS,
		idl.Substep_DELETE_MASTER_STATEDIR,
		idl.Substep_ARCHIVE_LOG_DIRECTORIES,
		idl.Substep_RESTORE_SOURCE_CLUSTER,
		idl.Substep_START_SOURCE_CLUSTER,
	})
	Help = map[string]string{
		"initialize": InitializeHelp,
		"execute":    ExecuteHelp,
		"finalize":   FinalizeHelp,
		"revert":     RevertHelp,
	}
}

func BuildRootCommand() *cobra.Command {
	// TODO: if called without a subcommand, the cli prints a help message with timestamp.  Remove the timestamp.
	var shouldPrintVersion bool

	root := &cobra.Command{
		Use: "gpupgrade",
		Run: func(cmd *cobra.Command, args []string) {
			if shouldPrintVersion {
				printVersion()
				return
			}

			fmt.Print(GlobalHelp)
		},
	}

	root.Flags().BoolVarP(&shouldPrintVersion, "version", "V", false, "prints version")

	root.AddCommand(config)
	root.AddCommand(version())
	root.AddCommand(initialize())
	root.AddCommand(execute())
	root.AddCommand(finalize())
	root.AddCommand(revert())
	root.AddCommand(restartServices)
	root.AddCommand(killServices)
	root.AddCommand(Agent())
	root.AddCommand(Hub())

	subConfigShow := createConfigShowSubcommand()
	config.AddCommand(subConfigShow)

	return addHelpToCommand(root, GlobalHelp)
}

// connTimeout retrieves the GPUPGRADE_CONNECTION_TIMEOUT environment variable,
// interprets it as a (possibly fractional) number of seconds, and converts it
// into a Duration. The default is one second if the envvar is unset or
// unreadable.
//
// TODO: should we make this a global --option instead?
func connTimeout() time.Duration {
	const defaultDuration = time.Second

	seconds, ok := os.LookupEnv("GPUPGRADE_CONNECTION_TIMEOUT")
	if !ok {
		return defaultDuration
	}

	duration, err := strconv.ParseFloat(seconds, 64)
	if err != nil {
		gplog.Warn(`GPUPGRADE_CONNECTION_TIMEOUT of "%s" is invalid (%s); using default of one second`,
			seconds, err)
		return defaultDuration
	}

	return time.Duration(duration * float64(time.Second))
}

// This reads the hub's persisted configuration for the current
// port.  If tryDefault is true and the configuration file does not exist,
// it will use the default port.  This might be the case if the hub is
// still running, even though the state directory, which contains the
// hub's persistent configuration, has been deleted.
// Any errors result in an os.Exit(1).
// NOTE: This overloads the hub's persisted configuration with that of the
// CLI when ideally these would be separate.
func getHubPort(tryDefault bool) int {
	conf := &hub.Config{}
	err := hub.LoadConfig(conf, upgrade.GetConfigFile())

	var pathError *os.PathError
	if tryDefault && xerrors.As(err, &pathError) {
		conf.Port = upgrade.DefaultHubPort
	} else if err != nil {
		gplog.Error("failed to retrieve hub port due to %v", err)
		os.Exit(1)
	}

	return conf.Port
}

// calls connectToHubOnPort() using the port defined in the configuration file
func connectToHub() (idl.CliToHubClient, error) {
	return connectToHubOnPort(getHubPort(false))
}

// connectToHubOnPort() performs a blocking connection to the hub based on the
// passed in port, and returns a CliToHubClient which wraps the resulting gRPC channel.
// Any errors result in a call to os.Exit(1).
func connectToHubOnPort(port int) (idl.CliToHubClient, error) {
	// Set up our timeout.
	ctx, cancel := context.WithTimeout(context.Background(), connTimeout())
	defer cancel()

	// Attempt a connection.
	address := "localhost:" + strconv.Itoa(port)
	conn, err := grpc.DialContext(ctx, address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		// Print a nicer error message if we can't connect to the hub.
		if ctx.Err() == context.DeadlineExceeded {
			gplog.Error("could not connect to the upgrade hub (did you run 'gpupgrade initialize'?)")
		}
		return nil, xerrors.Errorf("connecting to hub on port %d: %w", port, err)
	}

	return idl.NewCliToHubClient(conn), nil
}

//////////////////////////////////////// CONFIG and its subcommands
var config = &cobra.Command{
	Use:   "config",
	Short: "subcommands to set parameters for subsequent gpupgrade commands",
	Long:  "subcommands to set parameters for subsequent gpupgrade commands",
}

func createConfigShowSubcommand() *cobra.Command {
	subShow := &cobra.Command{
		Use:   "show",
		Short: "show configuration settings",
		Long:  "show configuration settings",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := connectToHub()
			if err != nil {
				return err
			}

			// Build a list of GetConfigRequests, one for each flag. If no flags
			// are passed, assume we want to retrieve all of them.
			var requests []*idl.GetConfigRequest
			getRequest := func(flag *pflag.Flag) {
				if flag.Name != "help" {
					requests = append(requests, &idl.GetConfigRequest{
						Name: flag.Name,
					})
				}
			}

			if cmd.Flags().NFlag() > 0 {
				cmd.Flags().Visit(getRequest)
			} else {
				cmd.Flags().VisitAll(getRequest)
			}

			// Make the requests and print every response.
			for _, request := range requests {
				resp, err := client.GetConfig(context.Background(), request)
				if err != nil {
					return err
				}

				if cmd.Flags().NFlag() == 1 {
					// Don't prefix with the setting name if the user only asked for one.
					fmt.Println(resp.Value)
				} else {
					fmt.Printf("%s - %s\n", request.Name, resp.Value)
				}
			}

			return nil
		},
	}

	subShow.Flags().Bool("id", false, "show upgrade identifier")
	subShow.Flags().Bool("source-gphome", false, "show path for the source Greenplum installation")
	subShow.Flags().Bool("target-gphome", false, "show path for the target Greenplum installation")
	subShow.Flags().Bool("target-datadir", false, "show temporary data directory for target gpdb cluster")

	return subShow
}

//////////////////////////////////////// VERSION
func version() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Version of gpupgrade",
		Long:  `Version of gpupgrade`,
		Run: func(cmd *cobra.Command, args []string) {
			printVersion()
		},
	}
}

//
// Upgrade Steps
//

func initialize() *cobra.Command {
	var file string
	var sourceGPHome, targetGPHome string
	var sourcePort int
	var hubPort int
	var agentPort int
	var diskFreeRatio float64
	var stopBeforeClusterCreation bool
	var verbose bool
	var ports string
	var mode string

	subInit := &cobra.Command{
		Use:   "initialize",
		Short: "prepare the system for upgrade",
		Long:  InitializeHelp,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// mark the required flags when the file flag is not set
			if !cmd.Flag("file").Changed {
				cmd.MarkFlagRequired("source-gphome")      //nolint
				cmd.MarkFlagRequired("target-gphome")      //nolint
				cmd.MarkFlagRequired("source-master-port") //nolint
			}

			// If the file flag is set check that no other flags are specified
			// other than verbose.
			if cmd.Flag("file").Changed {
				var err error
				cmd.Flags().Visit(func(flag *pflag.Flag) {
					if flag.Name != "file" && flag.Name != "verbose" {
						err = errors.New("The file flag cannot be used with any other flag.")
					}
				})
				return err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if cmd.Flag("file").Changed {
				configFile, err := os.Open(file)
				if err != nil {
					return err
				}
				defer func() {
					if cErr := configFile.Close(); cErr != nil {
						err = errorlist.Append(err, cErr)
					}
				}()

				flags, err := ParseConfig(configFile)
				if err != nil {
					return xerrors.Errorf("in file %q: %w", file, err)
				}

				err = addFlags(cmd, flags)
				if err != nil {
					return err
				}
			}

			linkMode, err := isLinkMode(mode)
			if err != nil {
				return err
			}

			// if diskFreeRatio is not explicitly set, use defaults
			if !cmd.Flag("disk-free-ratio").Changed {
				if linkMode {
					diskFreeRatio = 0.2
				} else {
					diskFreeRatio = 0.6
				}
			}

			if diskFreeRatio < 0.0 || diskFreeRatio > 1.0 {
				// Match Cobra's option-error format.
				return fmt.Errorf(
					`invalid argument %g for "--disk-free-ratio" flag: value must be between 0.0 and 1.0`,
					diskFreeRatio,
				)
			}

			ports, err := parsePorts(ports)
			if err != nil {
				return err
			}

			// If we got here, the args are okay and the user doesn't need a usage
			// dump on failure.
			cmd.SilenceUsage = true

			// Past this point, we want any errors to be accompanied by helper
			// text describing the next actions to take.
			defer func() {
				if err != nil {
					// XXX work around an annoying UI nit: the error message is
					// smashed against the failing substep without any vertical
					// space. Ideally this would be handled by the "UI" logic as
					// opposed to pushed into business logic.
					fmt.Println()

					err = cli.NewNextActions(err, "initialize")
				}
			}()

			// Check for valid versions BEFORE running any initialize
			// implementation code; why boot a hub if the versions are wrong?
			if err := cli.ValidateVersions(sourceGPHome, targetGPHome); err != nil {
				return err
			}

			fmt.Println()
			fmt.Println("Initialize in progress.")
			fmt.Println()

			timer := stopwatch.Start()
			operation := strings.Title(strings.ToLower(idl.Step_INITIALIZE.String()))
			defer func() {
				if err != nil {
					commanders.LogDuration(operation, verbose, timer.Stop())
				}
			}()

			s := commanders.NewSubstep(idl.Substep_CREATING_DIRECTORIES, verbose)
			err = commanders.CreateStateDir()
			s.Finish(&err)
			if err != nil {
				return xerrors.Errorf("create state directory: %w", err)
			}

			err = commanders.CreateInitialClusterConfigs(hubPort)
			if err != nil {
				return xerrors.Errorf("create initial cluster configs: %w", err)
			}

			s = commanders.NewSubstep(idl.Substep_START_HUB, verbose)
			err = commanders.StartHub()
			s.Finish(&err)
			if err != nil {
				return xerrors.Errorf("start hub: %w", err)
			}

			client, err := connectToHub()
			if err != nil {
				return err
			}

			request := &idl.InitializeRequest{
				AgentPort:    int32(agentPort),
				SourceGPHome: filepath.Clean(sourceGPHome),
				TargetGPHome: filepath.Clean(targetGPHome),
				SourcePort:   int32(sourcePort),
				UseLinkMode:  linkMode,
				Ports:        ports,
			}
			err = commanders.Initialize(client, request, verbose)
			if err != nil {
				return xerrors.Errorf("initialize hub: %w", err)
			}

			s = commanders.NewSubstep(idl.Substep_CHECK_DISK_SPACE, verbose)
			err = commanders.RunChecks(client, diskFreeRatio)
			s.Finish(&err)
			if err != nil {
				return err
			}

			if stopBeforeClusterCreation {
				return nil
			}

			err = commanders.InitializeCreateCluster(client, verbose)
			if err != nil {
				return xerrors.Errorf("initialize create cluster: %w", err)
			}

			commanders.LogDuration(operation, verbose, timer.Stop())

			fmt.Print(`
Initialize completed successfully.

NEXT ACTIONS
------------
To proceed with the upgrade, run "gpupgrade execute"
followed by "gpupgrade finalize".

To return the cluster to its original state, run "gpupgrade revert".
`)

			return nil
		},
	}
	subInit.Flags().StringVarP(&file, "file", "f", "", "the configuration file to use")
	subInit.Flags().StringVar(&sourceGPHome, "source-gphome", "", "path for the source Greenplum installation")
	subInit.Flags().StringVar(&targetGPHome, "target-gphome", "", "path for the target Greenplum installation")
	subInit.Flags().IntVar(&sourcePort, "source-master-port", 5432, "master port for source gpdb cluster")
	subInit.Flags().IntVar(&hubPort, "hub-port", upgrade.DefaultHubPort, "the port gpupgrade hub uses to listen for commands on")
	subInit.Flags().IntVar(&agentPort, "agent-port", upgrade.DefaultAgentPort, "the port gpupgrade agent uses to listen for commands on")
	subInit.Flags().BoolVar(&stopBeforeClusterCreation, "stop-before-cluster-creation", false, "only run up to pre-init")
	subInit.Flags().MarkHidden("stop-before-cluster-creation") //nolint
	subInit.Flags().Float64Var(&diskFreeRatio, "disk-free-ratio", 0.60, "percentage of disk space that must be available (from 0.0 - 1.0)")
	subInit.Flags().BoolVarP(&verbose, "verbose", "v", false, "print the output stream from all substeps")
	subInit.Flags().StringVar(&ports, "temp-port-range", "", "set of ports to use when initializing the target cluster")
	subInit.Flags().StringVar(&mode, "mode", "copy", "performs upgrade in either copy or link mode. Default is copy.")
	return addHelpToCommand(subInit, InitializeHelp)
}

func execute() *cobra.Command {
	var verbose bool

	cmd := &cobra.Command{
		Use:   "execute",
		Short: "executes the upgrade",
		Long:  ExecuteHelp,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			cmd.SilenceUsage = true

			timer := stopwatch.Start()
			operation := strings.Title(strings.ToLower(idl.Step_EXECUTE.String()))
			defer func() {
				if err != nil {
					commanders.LogDuration(operation, verbose, timer.Stop())
				}
			}()

			client, err := connectToHub()
			if err != nil {
				return err
			}

			response, err := commanders.Execute(client, verbose)
			if err != nil {
				return err
			}

			commanders.LogDuration(operation, verbose, timer.Stop())

			message := fmt.Sprintf(`
Execute completed successfully.

The target cluster is now running. The PGPORT is %s and the 
MASTER_DATA_DIRECTORY is %s

You may now run queries against the target database and perform any other 
validation desired prior to finalizing your upgrade.

WARNING: If any queries modify the target database prior to gpupgrade finalize, 
it will be inconsistent with the source database. 

NEXT ACTIONS
------------
If you are satisfied with the state of the cluster, run "gpupgrade finalize" 
to proceed with the upgrade.

To return the cluster to its original state, run "gpupgrade revert".
`, response.TargetPort, response.TargetMasterDataDir)

			fmt.Print(message)
			return nil
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "print the output stream from all substeps")

	return addHelpToCommand(cmd, ExecuteHelp)
}

func finalize() *cobra.Command {
	var verbose bool

	cmd := &cobra.Command{
		Use:   "finalize",
		Short: "finalizes the cluster after upgrade execution",
		Long:  FinalizeHelp,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			timer := stopwatch.Start()
			operation := strings.Title(strings.ToLower(idl.Step_FINALIZE.String()))
			defer func() {
				if err != nil {
					commanders.LogDuration(operation, verbose, timer.Stop())
				}
			}()

			client, err := connectToHub()
			if err != nil {
				return err
			}
			response, err := commanders.Finalize(client, verbose)
			if err != nil {
				return err
			}

			commanders.LogDuration(operation, verbose, timer.Stop())

			message := fmt.Sprintf(`
Finalize completed successfully.

The target cluster is now upgraded and is ready to be used. The PGPORT is %s and the MASTER_DATA_DIRECTORY is %s.
`, response.TargetPort, response.TargetMasterDataDir)

			fmt.Print(message)
			return nil
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "print the output stream from all substeps")

	return addHelpToCommand(cmd, FinalizeHelp)
}

func revert() *cobra.Command {
	var verbose bool

	cmd := &cobra.Command{
		Use:   "revert",
		Short: "reverts the upgrade and returns the cluster to its original state",
		Long:  RevertHelp,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			timer := stopwatch.Start()
			operation := strings.Title(strings.ToLower(idl.Step_REVERT.String()))
			defer func() {
				if err != nil {
					commanders.LogDuration(operation, verbose, timer.Stop())
				}
			}()

			client, err := connectToHub()
			if err != nil {
				return err
			}

			response, err := commanders.Revert(client, verbose)
			if err != nil {
				return err
			}

			s := commanders.NewSubstep(idl.Substep_STOP_HUB_AND_AGENTS, verbose)
			err = stopHubAndAgents(false)
			s.Finish(&err)
			if err != nil {
				return err
			}

			s = commanders.NewSubstep(idl.Substep_DELETE_MASTER_STATEDIR, verbose)
			streams := &step.BufferedStreams{}
			err = upgrade.DeleteDirectories([]string{utils.GetStateDir()}, upgrade.StateDirectoryFiles, streams)
			if verbose {
				os.Stdout.Write(streams.StdoutBuf.Bytes())
				os.Stdout.Write(streams.StderrBuf.Bytes())
			}
			s.Finish(&err)
			if err != nil {
				return err
			}

			commanders.LogDuration(operation, verbose, timer.Stop())

			fmt.Printf(`
Revert completed successfully.

Reverted to source cluster version %s.

The source cluster is now running. The PGPORT is %s and the 
MASTER_DATA_DIRECTORY is %s

The gpupgrade logs can be found on the master and segment hosts in
%s

NEXT ACTIONS
------------
To restart the upgrade, run "gpupgrade initialize" again.

To use the reverted cluster, you must recreate any tables, indexes, and/or 
roles that were dropped or altered to pass the pg_upgrade checks.
`, response.Version, response.SourcePort, response.SourceMasterDataDir, response.ArchiveDir)

			return nil
		},
	}

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "print the output stream from all substeps")

	return addHelpToCommand(cmd, RevertHelp)
}

func parsePorts(val string) ([]uint32, error) {
	var ports []uint32

	if val == "" {
		return ports, nil
	}

	for _, p := range strings.Split(val, ",") {
		parts := strings.Split(p, "-")
		switch {
		case len(parts) == 2: // this is a range
			low, err := strconv.ParseUint(parts[0], 10, 16)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse port range %s", p)
			}

			high, err := strconv.ParseUint(parts[1], 10, 16)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse port range %s", p)
			}

			if low > high {
				return nil, xerrors.Errorf("invalid port range %s", p)
			}

			for i := low; i <= high; i++ {
				ports = append(ports, uint32(i))
			}

		default: // single port
			port, err := strconv.ParseUint(p, 10, 16)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse port %s", p)
			}

			ports = append(ports, uint32(port))
		}
	}

	return ports, nil
}

// isLinkMode parses the mode flag returning an error if it is not copy or link.
// It returns true if mode is link.
func isLinkMode(input string) (bool, error) {
	choices := []string{"copy", "link"}

	mode := strings.ToLower(strings.TrimSpace(input))
	for _, choice := range choices {
		if mode == choice {
			return mode == "link", nil
		}
	}

	return false, fmt.Errorf("Invalid input %q. Please specify either %s.", input, strings.Join(choices, " or "))
}

func addFlags(cmd *cobra.Command, flags map[string]string) error {
	for name, value := range flags {
		flag := cmd.Flag(name)
		if flag == nil {
			var names []string
			cmd.Flags().VisitAll(func(flag *pflag.Flag) {
				names = append(names, flag.Name)
			})
			return xerrors.Errorf("The configuration parameter %q was not found in the list of supported parameters: %s.", name, strings.Join(names, ", "))
		}

		err := flag.Value.Set(value)
		if err != nil {
			return xerrors.Errorf("set %q to %q: %w", name, value, err)
		}

		cmd.Flag(name).Changed = true
	}

	return nil
}

var restartServices = &cobra.Command{
	Use:   "restart-services",
	Short: "restarts hub/agents that are not currently running",
	Long:  "restarts hub/agents that are not currently running",
	RunE: func(cmd *cobra.Command, args []string) error {
		running, err := commanders.IsHubRunning()
		if err != nil {
			return xerrors.Errorf("failed to determine if there is a hub running: %w", err)
		}

		if !running {
			err = commanders.StartHub()
			if err != nil {
				return err
			}
			fmt.Println("Restarted hub")
		}

		client, err := connectToHub()
		if err != nil {
			return err
		}

		reply, err := client.RestartAgents(context.Background(), &idl.RestartAgentsRequest{})
		if err != nil {
			return xerrors.Errorf("restarting agents: %w", err)
		}

		for _, host := range reply.GetAgentHosts() {
			fmt.Printf("Restarted agent on: %s\n", host)
		}

		return nil
	},
}

var killServices = &cobra.Command{
	Use:   "kill-services",
	Short: "Abruptly stops the hub and agents that are currently running.",
	Long: "Abruptly stops the hub and agents that are currently running.\n" +
		"Return if no hub is running, which may leave spurious agents running.",
	RunE: func(cmd *cobra.Command, args []string) error {
		running, err := commanders.IsHubRunning()
		if err != nil {
			return xerrors.Errorf("failed to determine if there is a hub running: %w", err)
		}

		if !running {
			// FIXME: Returning early if the hub is not running, means that we
			//  cannot kill spurious agents.
			// We cannot simply start the hub in order to kill spurious agents
			// since this requires initialize to have been run and the source
			// cluster config to exist. The main use case for kill-services is
			// at the start of BATS testing where we do not want to make any
			// assumption about the state of the cluster or environment.
			return nil
		}

		return stopHubAndAgents(true)
	},
}

func stopHubAndAgents(tryDefaultPort bool) error {
	client, err := connectToHubOnPort(getHubPort(tryDefaultPort))
	if err != nil {
		return err
	}

	_, err = client.StopServices(context.Background(), &idl.StopServicesRequest{})
	if err != nil {
		errCode := grpcStatus.Code(err)
		errMsg := grpcStatus.Convert(err).Message()
		// XXX: "transport is closing" is not documented but is needed to uniquely interpret codes.Unavailable
		// https://github.com/grpc/grpc/blob/v1.24.0/doc/statuscodes.md
		if errCode != codes.Unavailable || errMsg != "transport is closing" {
			return err
		}
	}
	return nil
}

const (
	initializeHelp = `
Runs pre-upgrade checks and prepares the cluster for upgrade.

Initialize will carry out the following steps:
%s

During or after gpupgrade initialize, you may revert the cluster to its
original state by running gpupgrade revert.

Usage: gpupgrade initialize --file <path/to/config_file>

Required Flags:

  -f, --file      config file containing upgrade parameters
                  (e.g. gpupgrade_config)

Optional Flags:

  -h, --help      displays help output for initialize

  -v, --verbose   outputs detailed logs for initialize

`
	executeHelp = `
Upgrades the master and primary segments to the target Greenplum version.

Execute will carry out the following steps:
%s

During or after gpupgrade execute, you may revert the cluster to its
original state by running gpupgrade revert.

Usage: gpupgrade execute

Optional Flags:

  -h, --help      displays help output for execute

  -v, --verbose   outputs detailed logs for execute

`
	finalizeHelp = `
Upgrades the standby master and mirror segments to the target Greenplum version.

Finalize will carry out the following steps:
%s

Once you run gpupgrade finalize, you may not revert the cluster to its
original state by running gpupgrade revert.

Usage: gpupgrade finalize

Optional Flags:

  -h, --help      displays help output for finalize

  -v, --verbose   outputs detailed logs for finalize

`
	revertHelp = `
Returns the cluster to its original state.
This command cannot be run after gpupgrade finalize has begun.

Revert will carry out some or all of the following steps:
%s
Usage: gpupgrade revert

Optional Flags:

  -h, --help      displays help output for revert
  -v, --verbose   outputs detailed logs for revert
`
	GlobalHelp = `
gpupgrade performs an in-place cluster upgrade to the next major version.

NOTE: Before running gpupgrade, you must prepare the cluster. Refer to
release notes for instructions.

Usage: gpupgrade [command] <flags> 

Required Commands: Run the three commands in this order

  1. initialize   runs pre-upgrade checks and prepares the cluster for upgrade

                  Usage: gpupgrade initialize --file </path/to/config_file>

                  Required Flags:
                    -f, --file   config file containing upgrade parameters
                                 (e.g. gpupgrade_config)

  2. execute      upgrades the master and primary segments to the target
                  Greenplum version

  3. finalize     upgrades the standby master and mirror segments to the target
                  Greenplum version

Optional Commands:

  revert          returns the cluster to its original state
                  Note: revert cannot be used after gpupgrade finalize

Optional Flags:

  -h, --help      displays help output for gpupgrade

  -v, --verbose   outputs detailed logs for gpupgrade

  -V, --version   displays the version of the current gpupgrade utility

Use "gpupgrade [command] --help" for more information about a command.

`
)

func GenerateHelpString(baseString string, commandList []idl.Substep) string {
	var formattedList string
	for _, substep := range commandList {
		formattedList += fmt.Sprintf(" - %s\n", commanders.SubstepDescriptions[substep].HelpText)
	}
	return fmt.Sprintf(baseString, formattedList)

}

// Cobra has multiple ways to handle help text, so we want to force all of them to use the same help text
func addHelpToCommand(cmd *cobra.Command, help string) *cobra.Command {
	// Add a "-?" flag, which Cobra does not provide by default
	var savedPreRunE func(cmd *cobra.Command, args []string) error
	var savedPreRun func(cmd *cobra.Command, args []string)
	if cmd.PreRunE != nil {
		savedPreRunE = cmd.PreRunE
	} else if cmd.PreRun != nil {
		savedPreRun = cmd.PreRun
	}

	var questionHelp bool
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if questionHelp {
			fmt.Print(help)
			os.Exit(0)
		}
		if savedPreRunE != nil {
			return savedPreRunE(cmd, args)
		} else if savedPreRun != nil {
			savedPreRun(cmd, args)
		}
		return nil
	}
	cmd.Flags().BoolVarP(&questionHelp, "?", "?", false, "displays help output")

	// Override the built-in "help" subcommand
	cmd.AddCommand(&cobra.Command{
		Use:   "help",
		Short: "",
		Long:  "",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Print(help)
			return nil
		},
	})
	cmd.SetUsageTemplate(help)

	// Override the built-in "-h" and "--help" flags
	cmd.SetHelpFunc(func(cmd *cobra.Command, strs []string) {
		fmt.Print(help)
	})

	return cmd
}
