# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

# log() prints its arguments to the TAP stream. Newlines are supported (each
# line will be correctly escaped in TAP).
log() {
    while read -r line; do
        echo "# $line" 1>&3
    done <<< "$*"
}

# fail() is meant to be called from BATS tests. It will fail the current test
# after printing its arguments to the TAP stream.
fail() {
    log "$@"
    false
}

# abort() is meant to be called from BATS tests. It will exit the process after
# printing its arguments to the TAP stream.
abort() {
    log "fatal: $*"
    exit 1
}

# skip_if_no_gpdb() will skip a test if a cluster's environment is not set up.
skip_if_no_gpdb() {
    [ -n "${GPHOME_SOURCE}" ] || skip "this test requires an active GPDB source cluster (set GPHOME_SOURCE)"
    [ -n "${GPHOME_TARGET}" ] || skip "this test requires an active GPDB target cluster (set GPHOME_TARGET)"
    [ -n "${PGPORT}" ] || skip "this test requires an active GPDB source cluster (set PGPORT)"
}

# start_source_cluster() ensures that database is up before returning
start_source_cluster() {
    if command -v "${GPHOME_SOURCE}"/bin/pg_isready > /dev/null; then
        "${GPHOME_SOURCE}"/bin/pg_isready -q || "${GPHOME_SOURCE}"/bin/gpstart -a
    else
        # 5X does not have pg_isready
        "${GPHOME_SOURCE}"/bin/psql postgres -qc "SELECT 1" &> /dev/null || "${GPHOME_SOURCE}"/bin/gpstart -a
    fi
}

# delete_cluster takes an master data directory and calls gpdeletesystem, and
# removes the associated data directories.
delete_cluster() {
    local gphome="$1"
    local masterdir="$2"

    # Perform a sanity check before deleting.
    expected_suffix="*qddir/demoDataDir.*.-1"
    [[ "$masterdir" == ${expected_suffix} ]] || \
        abort "cowardly refusing to delete $masterdir which does not look like an upgraded demo data directory. Expected suffix ${expected_suffix}"

    __gpdeletesystem "$gphome" "$masterdir"

    # XXX: Since gpugprade archives instead of removing data directories,
    # gpupgrade will fail when copying the master data directory to segments
    # with "file exists". To prevent this remove the data directories.
    delete_target_datadirs "$masterdir"
}

# delete_finalized_cluster takes an upgrade master data directory and deletes
# the cluster. It also resets the finalized data directories to what they were
# before upgrade by removing the upgraded data directories, and renaming the
# archive directories to their original name (which is the same as their
# upgraded name).
delete_finalized_cluster() {
    local gphome="$1"
    local masterdir="$2"

    # Perform a sanity check before deleting.
    local archive_masterdir=$(archive_dir "$masterdir")
    [ -d "$archive_masterdir" ] || abort "cowardly refusing to delete $masterdir. Expected $archive_masterdir to exist."

    __gpdeletesystem "$gphome" "$masterdir"

    local id=$(gpupgrade config show --id)

    local datadirs=$(dirname "$(dirname "$masterdir")")
    for archive in $(find "${datadirs}" -name "*${id}*.old"); do
        # The following sed matches archived data directories and returns the
        # path of the original directory. For example,
        #   /dbfast_mirror2/demoDataDir.BY6l9U0LfX8.1.old -> /dbfast_mirror2/demoDataDir1
        #   /datadirs/standby.BY6l9U0LfX8.old -> /datadirs/standby
        local original=$(sed -E 's/\.'"${id}"'(\.([-0-9]+))?\.old/\2/' <<< "$archive")
        rm -rf "${original}"
        mv "$archive" "$original"
    done
}

# Calls gpdeletesystem on the cluster pointed to by the given master data
# directory.
__gpdeletesystem() {
    local gphome="$1"
    local masterdir="$2"

    # Look up the master port (fourth line of the postmaster PID file).
    local port=$(awk 'NR == 4 { print $0 }' < "$masterdir/postmaster.pid")

    local gpdeletesystem="$gphome"/bin/gpdeletesystem

    # XXX gpdeletesystem returns 1 if there are warnings. There are always
    # warnings. So we ignore the exit code...
    (source $gphome/greenplum_path.sh && yes | PGPORT="$port" "$gpdeletesystem" -fd "$masterdir") || true
}

delete_target_datadirs() {
    local masterdir="$1"
    local datadir=$(dirname "$(dirname "$masterdir")")

    rm -rf "${datadir}"/*/demoDataDir.*.[0-9]
}

# require_gnu_stat tries to find a GNU stat program. If one is found, it will be
# assigned to the STAT global variable; otherwise the current test is skipped.
require_gnu_stat() {
    if command -v gstat > /dev/null; then
        STAT=gstat
    elif command -v stat > /dev/null; then
        STAT=stat
    else
        skip "GNU stat is required for this test"
    fi

    # Check to make sure what we have is really GNU.
    local version=$($STAT --version || true)
    [[ $version = *"GNU coreutils"* ]] || skip "GNU stat is required for this test"
}

process_is_running() {
    ps -ef | grep -wGc "$1"
}

# Takes an original datadir and echoes the expected temporary datadir containing
# the upgradeID.
#
# NOTE for devs: this is just for getting the expected data directories, which
# is an implementation detail. If you want the actual location of the new master
# data directory after an initialization, you can just ask the hub with
#
#    gpupgrade config show --target-datadir
#
expected_target_datadir() {
    local dir=$1
    local parentDir=$(dirname "${dir}")
    local baseDir=$(basename "${dir}")
    local suffix="${baseDir#demoDataDir}"

    local upgradeID
    upgradeID=$(gpupgrade config show --id)

    # Sanity check.
    [ -n "$parentDir" ]

    if [ "${baseDir}" == "standby" ]; then
        echo "${parentDir}/${baseDir}.${upgradeID}"
        return
    fi

    echo "${parentDir}/demoDataDir.${upgradeID}.${suffix}"
}

# archive_dir echoes the expected archive directory given an original data
# directory.
archive_dir() {
    local dir=$1
    echo "$(expected_target_datadir "$dir")".old
}

is_GPDB5() {
    local gphome=$1
    local version=$("$gphome"/bin/postgres --gp-version)

    [[ $version =~ ^"postgres (Greenplum Database) 5." ]]
}

# get_datadirs returns the datadirs across various version of GPDB.
# Arguments are GPHOME, PGPORT, and the WHERE clause to use when querying
# gp_segment_configuration.
query_datadirs() {
    local gphome=$1
    local port=$2
    local where_clause=${3:-true}

    local sql="SELECT datadir FROM gp_segment_configuration WHERE ${where_clause} ORDER BY content, role"

     if is_GPDB5 "$gphome"; then
        sql="SELECT e.fselocation as datadir
FROM gp_segment_configuration s
JOIN pg_filespace_entry e ON s.dbid = e.fsedbid
JOIN pg_filespace f ON e.fsefsoid = f.oid
WHERE f.fsname = 'pg_system' AND ${where_clause}
ORDER BY s.content, s.role"
    fi

    run "$gphome"/bin/psql -At -p "$port" postgres -c "$sql"
    [ "$status" -eq 0 ] || fail "$output"

    echo "$output"
}
