#! /usr/bin/env bats
#
# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

load helpers
load tablespace_helpers
load teardown_helpers

setup() {
    skip_if_no_gpdb

    HOSTS=($(all_hosts))
    setup_state_dirs "${HOSTS[@]}"

    gpupgrade kill-services

    PSQL="$GPHOME_SOURCE"/bin/psql
}


teardown() {
    skip_if_no_gpdb

    run_teardowns
}

setup_state_dirs() {
    local hosts=("$@")

    # Create a temporary state directory locally, on the master segment.
    STATE_DIR=`mktemp -d /tmp/gpupgrade.XXXXXX`
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"

    # The hosts list will contain the master host, which we already created a
    # directory for. That's okay; mkdir -p will ignore it. We still need the
    # teardown.
    for host in "${hosts[@]}"; do
        ssh "$host" mkdir -p "$STATE_DIR"
        register_teardown ssh "$host" rm -r "$STATE_DIR"
    done
}

# Prints all unique hostnames in the source cluster, one per line.
all_hosts() {
    # Use GROUP BY/ORDER BY MIN() rather than SELECT DISTINCT so that results
    # are ordered by dbid; that's the host order most devs are used to for test
    # clusters.
    "$GPHOME_SOURCE"/bin/psql -At postgres -c "
        SELECT hostname
          FROM gp_segment_configuration
         GROUP BY hostname
         ORDER BY MIN(dbid);
    "
}

host_process_is_running() {
    local host=$1
    local pattern=$2

    ssh "$host" "ps -ef | grep -wGc '$pattern'"
}

# query_host_datadirs returns a host/datadir pair for each segment in the
# cluster. Each pair is on its own line, separated by a tab. Arguments are
# GPHOME, PGPORT, and an optional WHERE clause to use when querying
# gp_segment_configuration.
query_host_datadirs() {
    local gphome=$1
    local port=$2
    local where_clause=${3:-true}

    local sql="SELECT hostname, datadir FROM gp_segment_configuration WHERE ${where_clause} ORDER BY content, role"

     if is_GPDB5 "$gphome"; then
        sql="
        SELECT s.hostname,
               e.fselocation as datadir
        FROM gp_segment_configuration s
        JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
        JOIN pg_filespace f ON e.fsefsoid = f.oid
        WHERE f.fsname = 'pg_system' AND ${where_clause}
        ORDER BY s.content, s.role"
    fi

    run "$gphome"/bin/psql -AtF$'\t' -p "$port" postgres -c "$sql"
    [ "$status" -eq 0 ] || fail "$output"

    echo "$output"
}

@test "reverting after initialize succeeds" {
    local target_hosts_dirs upgradeID

    gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose 3>&-

    # grab cluster data before revert destroys it
    target_hosts_dirs=$(jq -r '.Target.Primaries[] | .Hostname + " " + .DataDir' "${GPUPGRADE_HOME}/config.json")
    upgradeID=$(gpupgrade config show --id)

    gpupgrade revert --verbose

    # gpupgrade processes are stopped
    ! process_is_running "[g]pupgrade hub" || fail 'expected hub to have been stopped'
    for host in "${HOSTS[@]}"; do
        ! host_process_is_running "$host" "[g]pupgrade agent" || fail "expected agent to have been stopped on host ${host}"
    done

    # target data directories are deleted
    while read -r host datadir; do
        ssh -n "$host" "[ ! -d '$datadir' ]" || fail "expected datadir ${host}:${datadir} to have been deleted"
    done <<< "${target_hosts_dirs}"

    # the GPUPGRADE_HOME directory is deleted
    for host in "${HOSTS[@]}"; do
        ssh "$host" "[ ! -d '$GPUPGRADE_HOME' ]" || fail "expected GPUPGRADE_HOME directory ${host}:${GPUPGRADE_HOME} to have been deleted"
    done

    # check that the archived log directory corresponds to this tests upgradeID
    if [[ -z $(find "${HOME}/gpAdminLogs/gpupgrade-${upgradeID}-"* -type d) ]]; then
        fail "expected the log directory to be archived and match ${HOME}/gpAdminLogs/gpupgrade-*"
    fi
}

test_revert_after_execute() {
    local mode="$1"
    local target_master_port=6020
    local old_config new_config mirrors primaries rows host datadir

    # Save segment configuration
    old_config=$(get_segment_configuration "${GPHOME_SOURCE}")

    # Place marker files on mirrors
    MARKER=source-cluster.MARKER
    mirrors=$(query_host_datadirs "$GPHOME_SOURCE" "$PGPORT" "role='m'")
    while read -r host datadir; do
        ssh -n "$host" touch "$datadir/${MARKER}"
    done <<< "$mirrors"

    # Cleanup marker files in all directories since on success link mode rsyncs
    # the marker file to primaries, and the test can fail at any point.
    local segments
    segments=$(query_host_datadirs "$GPHOME_SOURCE" "$PGPORT")
    while read -r host datadir; do
        register_teardown ssh "$host" rm -f "$datadir/${MARKER}"
    done <<< "$segments"

    # Add a tablespace, which only works when upgrading from 5X.
    if is_GPDB5 "$GPHOME_SOURCE"; then
        local tablespace_table="tablespace_table"
        create_tablespace_with_table "$tablespace_table"
        register_teardown delete_tablespace_data "$tablespace_table"
    fi

    # Add a table
    TABLE="should_be_reverted"
    $PSQL postgres -c "CREATE TABLE ${TABLE} (a INT)"
    register_teardown $PSQL postgres -c "DROP TABLE ${TABLE}"

    $PSQL postgres -c "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

    gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range ${target_master_port}-6040 \
        --disk-free-ratio 0 \
        --mode "$mode" \
        --verbose 3>&-
    gpupgrade execute --verbose

    # Modify the table on the target cluster
    $PSQL -p $target_master_port postgres -c "TRUNCATE ${TABLE}"

    # Modify the table in the tablespace on the target cluster
    # Note: tablespace only work when upgrading from 5X.
    if is_GPDB5 "$GPHOME_SOURCE"; then
        $PSQL -p $target_master_port postgres -c "TRUNCATE $tablespace_table"
    fi

    # Revert
    gpupgrade revert --verbose

    # Verify the table modifications were reverted
    rows=$($PSQL postgres -Atc "SELECT COUNT(*) FROM ${TABLE}")
    if (( rows != 3 )); then
        fail "table ${TABLE} truncated after execute was not reverted: got $rows rows want 3"
    fi

    if is_GPDB5 "$GPHOME_SOURCE"; then
        check_tablespace_data "$tablespace_table"
    fi

    # Verify marker files on primaries
    primaries=$(query_host_datadirs $GPHOME_SOURCE $PGPORT "role='p'")
    while read -r host datadir; do
        if [ "$mode" = "link" ]; then
            ssh -n "$host" "[ -f '${datadir}/${MARKER}' ]" || fail "in link mode using rsync expected ${MARKER} marker file to be in datadir: $host:$datadir"
        else
            ssh -n "$host" "[ ! -f '${datadir}/${MARKER}' ]" || fail "in copy mode using gprecoverseg unexpected ${MARKER} marker file in datadir: $host:$datadir"
        fi
    done <<< "$primaries"

    # Check that transactions can be started on the source
    $PSQL postgres --single-transaction -c "SELECT version()" || fail "unable to start transaction"

    # Check to make sure the old cluster still matches
    new_config=$(get_segment_configuration "${GPHOME_SOURCE}")
    [ "$old_config" = "$new_config" ] || fail "actual config: $new_config, wanted: $old_config"

    # ensure target cluster is down
    ! isready "${GPHOME_TARGET}" ${target_master_port} || fail "expected target cluster to not be running on port ${target_master_port}"

    is_source_standby_in_sync || fail "expected standby to eventually be in sync"
}

@test "reverting after execute in link mode succeeds" {
    test_revert_after_execute "link"
}

@test "reverting after execute in copy mode succeeds" {
    test_revert_after_execute "copy"
}

@test "can successfully run gpupgrade after a revert" {
    gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose 3>&-

    gpupgrade execute --verbose

    gpupgrade revert --verbose

    gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose 3>&-

    gpupgrade execute --verbose

    # This last revert is used for test cleanup.
    gpupgrade revert --verbose
}

# gp_segment_configuration does not show us the status correctly. We must check that the
# sent_location from the master equals the replay_location of the standby.
is_source_standby_in_sync() {
    local INSYNC="f"
    local duration=600 # wait up to 10 minutes
    local poll=5

    while (( duration > 0 )); do
        INSYNC=$("$PSQL" -AXt postgres -c "SELECT sent_location=replay_location FROM pg_stat_replication")

        if [[ -z "$INSYNC" ]] && ! is_source_standby_running; then
            break # standby has disappeared
        elif [[ "$INSYNC" == "t" ]]; then
            break
        fi

        sleep $poll
        (( duration = duration - poll ))
    done

    [[ $INSYNC == "t" ]]
}

is_source_standby_running() {
    local host datadir
    read -r host datadir <<<"$(query_host_datadirs "$GPHOME_SOURCE" "$PGPORT" "content = '-1' AND role = 'm'")"

    if ! ssh "$host" "${GPHOME_SOURCE}"/bin/pg_ctl status -D "$datadir" > /dev/null; then
        return 1
    fi
}

setup_master_upgrade_failure() {
    "$PSQL" postgres --single-transaction -f - <<"EOF"
        CREATE TABLE master_failure (a int, b int);
        INSERT INTO master_failure SELECT i, i FROM generate_series(1,10)i;
EOF

    register_teardown "$PSQL" postgres -c "DROP TABLE IF EXISTS master_failure"

    local file dboid
    file=$("$GPHOME_SOURCE"/bin/psql -d postgres -Atc "SELECT relfilenode FROM pg_class WHERE relname='master_failure';")
    dboid=$("$GPHOME_SOURCE"/bin/psql -d postgres -Atc "SELECT oid FROM pg_database WHERE datname='postgres';")
    mv "$MASTER_DATA_DIRECTORY/base/$dboid/$file" "$MASTER_DATA_DIRECTORY/base/$dboid/$file.bkp"
    register_teardown mv "$MASTER_DATA_DIRECTORY/base/$dboid/$file.bkp" "$MASTER_DATA_DIRECTORY/base/$dboid/$file"
}

test_revert_after_execute_master_failure() {
    local mode="$1"
    local target_master_port=6020
    local old_config new_config mirrors primaries rows

    # Save segment configuration
    old_config=$(get_segment_configuration "${GPHOME_SOURCE}")

    # Place marker files on mirrors
    MARKER=source-cluster.MARKER
    mirrors=($(query_datadirs $GPHOME_SOURCE $PGPORT "role='m'"))
    for datadir in "${mirrors[@]}"; do
        touch "$datadir/${MARKER}"
        register_teardown rm -f "$datadir/${MARKER}"
    done

    # Add a tablespace, which only works when upgrading from 5X.
    if is_GPDB5 "$GPHOME_SOURCE"; then
        local tablespace_table="tablespace_table"
        create_tablespace_with_table "$tablespace_table"
        register_teardown delete_tablespace_data "$tablespace_table"
    fi

    # Add a table
    TABLE="should_be_reverted"
    $PSQL postgres -c "CREATE TABLE ${TABLE} (a INT)"
    register_teardown $PSQL postgres -c "DROP TABLE ${TABLE}"

    $PSQL postgres -c "INSERT INTO ${TABLE} VALUES (1), (2), (3)"

    gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range ${target_master_port}-6040 \
        --disk-free-ratio 0 \
        --mode "$mode" \
        --verbose 3>&-

    # Execute should fail.
    local status=0
    gpupgrade execute --verbose || status=$?
    [ "$status" -ne 0 ] || fail "expected execute to fail"

    # Revert
    gpupgrade revert --verbose

    # Verify the table is untouched
    rows=$($PSQL postgres -Atc "SELECT COUNT(*) FROM ${TABLE}")
    if (( rows != 3 )); then
        fail "table ${TABLE} was not untouched: got $rows rows want 3"
    fi

    if is_GPDB5 "$GPHOME_SOURCE"; then
        check_tablespace_data "$tablespace_table"
    fi

    # Verify that the marker files do not exist on primaries. Unlike for the
    # successful execute case, a revert from a failed master upgrade should
    # never require an rsync from mirrors, since the master hasn't been started
    # yet.
    primaries=($(query_datadirs $GPHOME_SOURCE $PGPORT "role='p'"))
    for datadir in "${primaries[@]}"; do
        [ ! -f "${datadir}/${MARKER}" ] || fail "revert resulted in unexpected ${MARKER} marker file in datadir: $datadir"
    done

    # Check that transactions can be started on the source
    $PSQL postgres --single-transaction -c "SELECT version()" || fail "unable to start transaction"

    # Check to make sure the old cluster still matches
    new_config=$(get_segment_configuration "${GPHOME_SOURCE}")
    [ "$old_config" = "$new_config" ] || fail "actual config: $new_config, wanted: $old_config"

    # ensure target cluster is down
    ! isready "${GPHOME_TARGET}" ${target_master_port} || fail "expected target cluster to not be running on port ${target_master_port}"

    is_source_standby_in_sync || fail "expected standby to eventually be in sync"
}

@test "reverting succeeds after copy-mode execute fails during master upgrade" {
    setup_master_upgrade_failure
    test_revert_after_execute_master_failure copy
}

@test "reverting succeeds after link-mode execute fails during master upgrade" {
    setup_master_upgrade_failure
    test_revert_after_execute_master_failure link
}
