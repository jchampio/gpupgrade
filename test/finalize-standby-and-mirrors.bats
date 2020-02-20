#!/usr/bin/env bats

load helpers

setup_state_dir() {
    STATE_DIR=$(mktemp -d /tmp/gpupgrade.XXXXXX)
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"
}

teardown_new_cluster() {
    local NEW_CLUSTER="$(gpupgrade config show --new-datadir)"

    if [ -n "$NEW_CLUSTER" ]; then
        delete_cluster $NEW_CLUSTER
    fi
}

setup() {
    skip_if_no_gpdb

    setup_state_dir

    gpupgrade kill-services
}

teardown() {
    teardown_new_cluster
    gpupgrade kill-services
    gpstart -a
    echo "done"
}

#
# This test should probably become a journey test for
# more finalize steps that actually go through the process of
# finalizing a cluster (mirrors, etc)
#
@test "finalize brings up the standby and mirrors for the new cluster" {
    local source_mirrors_count=$(number_of_mirrors)
    gpupgrade initialize \
        --old-bindir="$GPHOME/bin" \
        --new-bindir="$GPHOME/bin" \
        --old-port="${PGPORT}" \
        --disk-free-ratio 0 \
        --verbose

    gpupgrade execute --verbose

    gpupgrade finalize

    local new_datadir=$(gpupgrade config show --new-datadir)
    local actual_standby_status=$(gpstate -d "${new_datadir}")
    local standby_status_line=$(get_standby_status "$actual_standby_status")
    [[ $standby_status_line == *"Standby host passive"* ]] || fail "expected standby to be up and in passive mode, got **** ${actual_standby_status} ****"

    local target_mirrors_count=$(number_of_mirrors)
    local gp_segment_configuration=$(psql postgres -c "select * from gp_segment_configuration")
    [[ $source_mirrors_count -eq $target_mirrors_count ]] || exit "expected target mirrors count '${target_mirrors_count}' to equal source mirrors count '${source_mirrors_count}'. gp_segment_configuration:
        ${gp_segment_configuration}"
}

number_of_mirrors() {
    # when the target cluster has finalized, it is running under the same PGPORT as the source cluster
    psql postgres --tuples-only --no-align -c "
        select count(*) from gp_segment_configuration
            where role='m' and status='u' and content != -1
    "
}

get_standby_status() {
    local standby_status=$1
    echo "$standby_status" | grep 'Standby master state'
}

