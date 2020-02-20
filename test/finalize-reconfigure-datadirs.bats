#!/usr/bin/env bats

load helpers

setup() {
    skip_if_no_gpdb

    [ ! -z $GPHOME ]
    GPHOME_NEW=${GPHOME_NEW:-$GPHOME}
    GPHOME_OLD=$GPHOME

    PSQL="$GPHOME_NEW/bin/psql --no-align --tuples-only postgres"

    setup_state_dir

    gpupgrade kill-services
}

teardown() {
    teardown_new_cluster
    gpupgrade kill-services

    # reload old path and start
    source "${GPHOME_OLD}/greenplum_path.sh"
    gpstart -a
}

@test "it swaps out the target cluster's data directories and archives the source cluster's data directories" {
    # place marker file in source master data directory
    local marker_file=source-cluster.test-marker
    touch "$MASTER_DATA_DIRECTORY/${marker_file}"

    # grab the original ports before starting so we can verify the target cluster
    # inherits the source cluster's ports
    local old_ports=$(get_ports)

    gpupgrade initialize \
        --old-bindir="$GPHOME/bin" \
        --new-bindir="$GPHOME_NEW/bin" \
        --old-port="${PGPORT}" \
        --disk-free-ratio 0 \
        --verbose

    gpupgrade execute --verbose

    gpupgrade finalize

    # ensure source cluster has been archived and target cluster is located where the source used to be
    local source_cluster_master_data_directory="${MASTER_DATA_DIRECTORY}_old"
    local target_cluster_master_data_directory="${MASTER_DATA_DIRECTORY}"
    [ -f "${source_cluster_master_data_directory}/${marker_file}" ] || fail "expected ${marker_file} marker file to be in source datadir: ${STATE_DIR}/base/demoDataDir-1"
    [ ! -f "${target_cluster_master_data_directory}/${marker_file}" ] || fail "unexpected ${marker_file} marker file in target datadir: ${STATE_DIR}/base/demoDataDir-1"

    # ensure gpperfmon configuration file has been modified to reflect new data dir location
    local gpperfmon_config_file="${target_cluster_master_data_directory}/gpperfmon/conf/gpperfmon.conf"
    grep "${target_cluster_master_data_directory}" "${gpperfmon_config_file}" || \
        fail "got gpperfmon.conf file $(cat $gpperfmon_config_file), wanted it to include ${target_cluster_master_data_directory}"

    # ensure that the new cluster is queryable, and has updated configuration
    segment_configuration=$($PSQL -c "select *, version() from gp_segment_configuration")
    [[ $segment_configuration == *"$target_cluster_master_data_directory"* ]] || fail "expected $segment_configuration to include $target_cluster_master_data_directory"

    # Check to make sure the new cluster's ports match the old one.
    local new_ports=$(get_ports)
    [ "$old_ports" = "$new_ports" ] || fail "actual ports: $new_ports, wanted $old_ports"
}

setup_state_dir() {
    STATE_DIR=$(mktemp -d /tmp/gpupgrade.XXXXXX)
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"
}

teardown_new_cluster() {
    delete_finalized_cluster $MASTER_DATA_DIRECTORY
}

# Writes the primary ports from the cluster pointed to by $PGPORT to stdout, one
# per line, sorted by content ID.
get_ports() {
    $PSQL -c "select content, role, port from gp_segment_configuration where role = 'p' order by content, role"
}
