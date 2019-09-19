#! /usr/bin/env bats

load helpers

setup() {
    STATE_DIR=`mktemp -d`
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"
    kill_hub
    kill_agents
}

teardown() {
    kill_hub
    kill_agents
    rm -r "$STATE_DIR"
}

@test "hub daemonizes and prints the PID when passed the --daemonize option" {
    run gpupgrade_hub --daemonize 3>&-
    [ "$status" -eq 0 ] || fail "$output"

    regex='pid ([[:digit:]]+)'
    [[ $output =~ $regex ]] || fail "actual output: $output"

    pid="${BASH_REMATCH[1]}"
    procname=$(ps -o ucomm= $pid)
    [ $procname = gpupgrade_hub ] || fail "actual process name: $procname"
}

@test "hub saves cluster configs to disk when initialized" {
    # XXX how useful is a test for this behavior?
    require_gpdb

    gpupgrade initialize \
        --old-bindir "$PWD" \
        --new-bindir "$PWD" \
        --old-port "$PGPORT" 3>&-

    [ -f "$GPUPGRADE_HOME"/source_cluster_config.json ]
    [ -f "$GPUPGRADE_HOME"/target_cluster_config.json ]
}

@test "substeps are marked complete after initialization" {
    require_gpdb

    gpupgrade initialize \
        --old-bindir=$PWD \
        --new-bindir=$PWD \
        --old-port=$PGPORT 3>&-

    run gpupgrade status upgrade
    [ "$status" -eq 0 ] || fail "$output"

    # XXX is this a useful test? Seems like it's pinning the wrong behavior.
    [ "${lines[0]}" = 'COMPLETE - Configuration Check' ] || fail "actual: ${lines[0]}"
    [ "${lines[1]}" = 'COMPLETE - Agents Started on Cluster' ] || fail "actual: ${lines[1]}"
}
