#! /usr/bin/env bats

load helpers

setup() {
    STATE_DIR=`mktemp -d`
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"
    gpupgrade initialize --old-bindir=/usr/local/gpdb6/bin/ --new-bindir=/usr/local/gpdb6/bin/

    kill_hub
}

teardown() {
    kill_hub
}

@test "start-hub fails if the source configuration hasn't been initialized" {
	rm $GPUPGRADE_HOME/source_cluster_config.json
    run gpupgrade prepare start-hub
    [ "$status" -eq 1 ]

    [[ "$output" = *"Unable to load source cluster configuration"* ]]
}

@test "start-hub fails if the target configuration hasn't been initialized" {
	rm $GPUPGRADE_HOME/target_cluster_config.json
    run gpupgrade prepare start-hub
    [ "$status" -eq 1 ]

    [[ "$output" = *"Unable to load target cluster configuration"* ]]
}

@test "initialize starts a daemonized gpupgrade hub process" {
    ps -ef | grep -Gq "[g]pupgrade hub --daemon$"
}

@test "initialize returns an error when it is ran twice" {
    # second start should return an error
    ! gpupgrade initialize --old-bindir=/usr/local/gpdb6/bin/ --new-bindir=/usr/local/gpdb6/bin/
    # TODO: check for a useful error message
}

@test "initialize does not return an error if an unrelated process has gpupgrade hub in its name" {
    # Create a long-running process with gpupgrade hub in the name.
    exec -a gpupgrade_hub_test_log sleep 5 3>&- &
    bgproc=$! # save the PID to kill later

    # Wait a little bit for the background process to get its new name.
    while ! ps -ef | grep -Gq "[g]pupgrade hub"; do
        sleep .001

        # To avoid hanging forever if something goes terribly wrong, make sure
        # the background process still exists during every iteration.
        kill -0 $bgproc
    done

    # Start the hub; there should be no errors.
    gpupgrade prepare start-hub 3>&-

    # Clean up. Use SIGINT rather than SIGTERM to avoid a nasty-gram from BATS.
    kill -INT $bgproc
}

@test "start-hub returns an error if gpupgrade hub isn't on the PATH" {
    # Save the path to gpupgrade, since Bash can't look it up once we clear PATH
    GPUPGRADE=`which gpupgrade`

    ! PATH= $GPUPGRADE prepare start-hub
    # TODO: check for a useful error message
}

outputContains() {
    [[ "$output" = *"$1"* ]]
}

@test "subcommands return an error if the hub is not started" {
    commands=(
        'prepare shutdown-clusters'
        'prepare start-agents'
        'prepare init-cluster'
        'config set --old-bindir /dummy'
        'config show'
        'check version'
        'check object-count'
        'check disk-space'
        'check config'
        'check seginstall'
        'status upgrade'
        'status conversion'
        'upgrade convert-master'
        'upgrade convert-primaries'
        'upgrade copy-master'
        'upgrade validate-start-cluster'
        'upgrade reconfigure-ports'
    )

    # We don't want to have to wait for the default one-second timeout for all
    # of these commands.
    export GPUPGRADE_CONNECTION_TIMEOUT=0

    # Run every subcommand.
    for command in "${commands[@]}"; do
        run gpupgrade $command

        # Trace which command we're on to make debugging easier.
        echo "\$ gpupgrade $command"
        echo "$output"

        [ "$status" -eq 1 ]
        outputContains "couldn't connect to the upgrade hub (did you run 'gpupgrade prepare start-hub'?)"
    done
}
