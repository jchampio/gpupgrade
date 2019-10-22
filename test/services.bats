#! /usr/bin/env bats

load helpers

setup() {
    skip_if_no_gpdb

    STATE_DIR=`mktemp -d`
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"

    gpupgrade kill-services

    gpupgrade initialize \
        --old-bindir="${GPHOME}/bin" \
        --new-bindir="${GPHOME}/bin" \
        --old-port="${PGPORT}" 3>&-
}

teardown() {
    # XXX Beware, BATS_TEST_SKIPPED is not a documented export.
    if [ -z "${BATS_TEST_SKIPPED}" ]; then
        gpupgrade kill-services
        rm -r "$STATE_DIR"
    fi
}

@test "gpupgrade stop-services actually stops hub and agents" {
    # check that hub and agent are up
    process_is_running "[g]pupgrade_hub"
    process_is_running "[g]pupgrade_agent"

    # stop them
    gpupgrade kill-services

    # make sure that they are down
    ! process_is_running "[g]pupgrade_hub"
    ! process_is_running "[g]pupgrade_agent"

    gpupgrade kill-services
}

@test "gpupgrade stop-services can be run multiple times without issue " {
    gpupgrade kill-services
    gpupgrade kill-services
}
