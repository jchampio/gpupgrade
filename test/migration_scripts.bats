#! /usr/bin/env bats
#
# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

load helpers

SCRIPTS_DIR=$BATS_TEST_DIRNAME/../migration_scripts

setup() {
    skip_if_no_gpdb

    STATE_DIR=`mktemp -d /tmp/gpupgrade.XXXXXX`
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"

    PSQL="$GPHOME_SOURCE/bin/psql -X --no-align --tuples-only"

    backup_source_cluster "$STATE_DIR"/backup

    TEST_DBNAME=testdb
    DEFAULT_DBNAME=postgres
    GPHDFS_USER=gphdfs_user

    $PSQL -c "DROP DATABASE IF EXISTS $TEST_DBNAME;" -d $DEFAULT_DBNAME
    $PSQL -c "DROP ROLE IF EXISTS $GPHDFS_USER;" -d $DEFAULT_DBNAME

    gpupgrade kill-services
}

teardown() {
    # XXX Beware, BATS_TEST_SKIPPED is not a documented export.
    if [ -n "${BATS_TEST_SKIPPED}" ]; then
        return
    fi

    if [ -n "$MIGRATION_DIR" ]; then
        rm -r $MIGRATION_DIR
    fi

    gpupgrade kill-services

    restore_source_cluster "$STATE_DIR"/backup
    rm -rf "$STATE_DIR"/backup

    rm -r "$STATE_DIR"
}

# XXX backup_source_cluster is a hack to work around the standby-revert bug.
# Instead of relying on revert to correctly reset the state of the standby, copy
# over the original cluster contents during teardown.
#
# Remove this and its companion ASAP.
backup_source_cluster() {
    local backup_dir=$1

    if [[ "$MASTER_DATA_DIRECTORY" != *"/datadirs/qddir/demoDataDir-1" ]]; then
        abort "refusing to back up cluster with master '$MASTER_DATA_DIRECTORY'; demo directory layout required"
    fi

    # Don't use -p. It's important that the backup directory not exist so that
    # we know we have control over it.
    mkdir "$backup_dir"

    local datadir_root
    datadir_root="$(realpath "$MASTER_DATA_DIRECTORY"/../..)"

    log "111111 $MASTER_DATA_DIRECTORY"
    log "222222 $datadir_root"

    gpstop -af
    rsync --archive "${datadir_root:?}"/ "${backup_dir:?}"/
    gpstart -a
}

# XXX restore_source_cluster is a hack to work around the standby-revert bug;
# see backup_source_cluster above
restore_source_cluster() {
    local backup_dir=$1

    if [[ "$MASTER_DATA_DIRECTORY" != *"/datadirs/qddir/demoDataDir-1" ]]; then
        abort "refusing to restore cluster with master '$MASTER_DATA_DIRECTORY'; demo directory layout required"
    fi

    local datadir_root
    datadir_root="$(realpath "$MASTER_DATA_DIRECTORY"/../..)"

    log "333333 $MASTER_DATA_DIRECTORY"
    log "444444 $datadir_root"

    stop_any_cluster
    rsync --archive -I --delete "${backup_dir:?}"/ "${datadir_root:?}"/
    gpstart -a
}

# stop_any_cluster will attempt to stop the cluster defined by MASTER_DATA_DIRECTORY.
stop_any_cluster() {
    local gphome
    gphome=$(awk '{ split($0, parts, "/bin/postgres"); print parts[1] }' "$MASTER_DATA_DIRECTORY"/postmaster.opts)

    (source "$gphome"/greenplum_path.sh && gpstop -af)
}

drop_unfixable_objects() {
    # the migration script should not remove primary / unique key constraints on partitioned tables, so
    # remove them manually by dropping the table as they can't be dropped.
    $GPHOME_SOURCE/bin/psql -d $TEST_DBNAME -p $PGPORT -c "DROP TABLE table_with_unique_constraint_p;"
    $GPHOME_SOURCE/bin/psql -d $TEST_DBNAME -p $PGPORT -c "DROP TABLE table_with_primary_constraint_p;"
}

@test "migration scripts generate sql to modify non-upgradeable objects and fix pg_upgrade check errors" {

    $PSQL -c "CREATE DATABASE $TEST_DBNAME;" -d $DEFAULT_DBNAME
    $PSQL -f $BATS_TEST_DIRNAME/../migration_scripts/test/create_nonupgradable_objects.sql -d $TEST_DBNAME

    run gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose
    echo "$output"
    [ "$status" -ne 0 ] || fail "expected initialize to fail due to pg_upgrade check"

    egrep "\"CHECK_UPGRADE\": \"FAILED\"" $GPUPGRADE_HOME/status.json
    egrep "^Checking.*fatal$" $GPUPGRADE_HOME/pg_upgrade/seg-1/pg_upgrade_internal.log

    MIGRATION_DIR=`mktemp -d /tmp/migration.XXXXXX`
    $SCRIPTS_DIR/generate_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR
    $SCRIPTS_DIR/execute_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR/pre-upgrade

    drop_unfixable_objects

    gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose
    gpupgrade execute --verbose
    gpupgrade finalize --verbose

    NEW_CLUSTER="$MASTER_DATA_DIRECTORY"
}

@test "after reverting recreate scripts restore dropped objects" {
    $PSQL -c "CREATE DATABASE $TEST_DBNAME;" -d $DEFAULT_DBNAME
    $PSQL -f $BATS_TEST_DIRNAME/../migration_scripts/test/create_nonupgradable_objects.sql -d $TEST_DBNAME

    drop_unfixable_objects # don't test what we won't fix

    # XXX We don't properly handle index constraints after revert, yet. Ignore
    # the test tables that break the diff for now.
    EXCLUSIONS="-T table_with_primary_constraint "
    EXCLUSIONS+="-T table_with_unique_constraint "
    EXCLUSIONS+="-T pt_with_index "
    EXCLUSIONS+="-T sales "

    MIGRATION_DIR=`mktemp -d /tmp/migration.XXXXXX`
    "$GPHOME_SOURCE"/bin/pg_dump --schema-only $TEST_DBNAME $EXCLUSIONS -f "$MIGRATION_DIR"/before.sql

    $SCRIPTS_DIR/generate_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR
    $SCRIPTS_DIR/execute_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR/pre-upgrade

    gpupgrade initialize \
        --source-gphome="$GPHOME_SOURCE" \
        --target-gphome="$GPHOME_TARGET" \
        --source-master-port="${PGPORT}" \
        --temp-port-range 6020-6040 \
        --disk-free-ratio 0 \
        --verbose
    gpupgrade execute --verbose
    gpupgrade revert --verbose

    $SCRIPTS_DIR/execute_migration_sql.bash $GPHOME_SOURCE $PGPORT $MIGRATION_DIR/post-revert

    "$GPHOME_SOURCE"/bin/pg_dump --schema-only $TEST_DBNAME $EXCLUSIONS -f "$MIGRATION_DIR"/after.sql
    diff -U3 --speed-large-files "$MIGRATION_DIR"/before.sql "$MIGRATION_DIR"/after.sql
}
