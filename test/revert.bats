#! /usr/bin/env bats
#
# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

load helpers

setup() {
    skip_if_no_gpdb

    STATE_DIR=`mktemp -d /tmp/gpupgrade.XXXXXX`
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"
    echo $GPUPGRADE_HOME

    PSQL="$GPHOME"/bin/psql
}

@test "reverting after initialize succeeds" {
    gpupgrade initialize \
        --source-bindir="$GPHOME/bin" \
        --target-bindir="$GPHOME/bin" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose 3>&-

    gpupgrade revert --verbose

    # gpupgrade processes are stopped
    ! process_is_running "[g]pupgrade hub" || fail 'expected hub to have been stopped'
    ! process_is_running "[g]pupgrade agent" || fail 'expected agent to have been stopped'

    # check that the target datadirs were deleted
    local target_hosts_dirs=$(jq -r '.Target.Primaries[] | .Hostname + " " + .DataDir' "${GPUPGRADE_HOME}/config.json")

    while read -r hostname datadir; do
        run ssh "${hostname}" stat "${datadir}"
        ! [ $status -eq 0 ] || fail "expected datadir ${datadir} to have been deleted"
    done <<< "${target_hosts_dirs}"

    # the GPUPGRADE_HOME directory is deleted
    if [ -d "${GPUPGRADE_HOME}" ]; then
        echo "expected GPUPGRADE_HOME directory ${GPUPGRADE_HOME} to have been deleted"
        exit 1
    fi

    # check that the archived log directory was created within the last 3 minutes
    if [[ -z $(find "${HOME}/gpAdminLogs/gpupgrade-"* -type d -cmin -3) ]]; then
        fail "expected the log directory to be archived and match ${HOME}/gpAdminLogs/gpupgrade-*"
    fi
}

@test "reverting after execute in copy mode succeeds" {
    local target_master_port=6020

    gpupgrade initialize \
        --source-bindir="$GPHOME/bin" \
        --target-bindir="$GPHOME/bin" \
        --source-master-port="${PGPORT}" \
        --temp-port-range ${target_master_port}-6040 \
        --disk-free-ratio 0 \
        --verbose 3>&-

    gpupgrade execute --verbose

    gpupgrade revert --verbose

    pg_isready -q || fail "expected source cluster to be running on port ${PGPORT}"
    ! pg_isready -qp ${target_master_port} || fail "expected target cluster to not be running on port ${target_master_port}"
}

@test "reverting after execute in link mode succeeds" {
    local target_master_port=6020
    local old_segconfig
    local new_segconfig

    old_segconfig=$($PSQL -Atc "SELECT * FROM gp_segment_configuration ORDER BY dbid" postgres)

    gpupgrade initialize \
        --source-bindir="$GPHOME/bin" \
        --target-bindir="$GPHOME/bin" \
        --source-master-port="${PGPORT}" \
        --temp-port-range ${target_master_port}-6040 \
        --disk-free-ratio 0 \
        --mode link \
        --verbose 3>&-
    gpupgrade execute --verbose

    gpupgrade revert --verbose

    # Check that the source is up and the target is down.
    pg_isready -q || fail "expected source cluster to be running on port ${PGPORT}"
    ! pg_isready -qp ${target_master_port} || fail "expected target cluster to not be running on port ${target_master_port}"

    # Check that transactions can be started on the source. (I.e. mirrors are
    # either functional or removed; we'll check for the latter below.)
    $PSQL --single-transaction -c 'SELECT version();' postgres || fail "unable to start transaction"

    # Check that the source has not changed.
    new_segconfig=$($PSQL -Atc "SELECT * FROM gp_segment_configuration ORDER BY dbid" postgres)

    if [ "$new_segconfig" != "$old_segconfig" ]; then
        echo "old configuration:"
        echo "$old_segconfig"
        echo "new configuration:"
        echo "$new_segconfig"

        fail "source cluster's segment configuration has changed"
    fi
}

@test "can successfully run gpupgrade after a revert" {
    gpupgrade initialize \
        --source-bindir="$GPHOME/bin" \
        --target-bindir="$GPHOME/bin" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose 3>&-

    gpupgrade execute --verbose

    gpupgrade revert --verbose

    gpupgrade initialize \
        --source-bindir="$GPHOME/bin" \
        --target-bindir="$GPHOME/bin" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose 3>&-

    gpupgrade execute --verbose

    # This last revert is used for test cleanup.
    gpupgrade revert --verbose
}
