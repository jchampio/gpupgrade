#! /usr/bin/env bats
#
# This file provides negative test cases for when the user does not execute
# upgrade steps in the correct order after starting the hub.

load helpers

setup() {
    create_test_directory

    gpupgrade kill-services

    gpupgrade initialize \
        --old-bindir="${GPHOME}/bin" \
        --new-bindir="${GPHOME}/bin" \
        --old-port="${PGPORT}" \
        --stop-before-cluster-creation \
        --disk-free-ratio 0 3>&-
}

teardown() {
    gpupgrade kill-services
    rm -r "${STATE_DIR}"
}

# todo: add tests
