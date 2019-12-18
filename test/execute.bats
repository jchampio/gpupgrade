#!/usr/bin/env bats

load helpers

setup() {
    skip_if_no_gpdb

    STATE_DIR=`mktemp -d`
    export GPUPGRADE_HOME="${STATE_DIR}/gpupgrade"

    gpupgrade kill-services
}

teardown() {
    skip_if_no_gpdb
    psql postgres -c "drop table if exists test_linking;"

    gpupgrade kill-services
    rm -r "$STATE_DIR"
}

ensure_hardlinks_for_relfilenode_on_master_and_segments() {
    local tablename=$1
    local expected_number_of_hardlinks=$2

    psql postgres -c "
      create or replace function get_relfilenode_for_table_for_segments(tablename text) returns setof text as \$\$
           select current_setting('data_directory') || '/' || pg_relation_filepath(tablename);
      \$\$ LANGUAGE SQL EXECUTE ON ALL SEGMENTS;

      create or replace function get_relfilenode_for_table_for_master(tablename text) returns setof text as \$\$
             select current_setting('data_directory') || '/' || pg_relation_filepath(tablename);
      \$\$ LANGUAGE SQL EXECUTE ON MASTER;

      create or replace function get_relfilenode_for_table(tablename text) returns setof text as \$\$
             select get_relfilenode_for_table_for_master(tablename) union all
                    select get_relfilenode_for_table_for_segments(tablename);
      \$\$ LANGUAGE SQL;
    "

    read -a relfilenodes <<< $(psql postgres --tuples-only --no-align -c " select get_relfilenode_for_table('$tablename');")

    for relfilenode in "${relfilenodes[@]}"; do
      local number_of_hardlinks=$($STAT --format "%h" "${relfilenode}")
      [ $number_of_hardlinks -eq $expected_number_of_hardlinks ]
    done
}

@test "gpupgrade execute should remember that link mode was specified in initialize" {
    require_gnu_stat

    # Setup
    psql postgres -c "create table test_linking (a int);"

    ensure_hardlinks_for_relfilenode_on_master_and_segments 'test_linking' 1

    # Run upgrade steps
    gpupgrade initialize \
        --old-bindir="$GPHOME/bin" \
        --new-bindir="$GPHOME/bin" \
        --old-port="${PGPORT}" \
        --link \
        --verbose

    gpupgrade execute --verbose

    # expect link mode to have created a hardlink
    ensure_hardlinks_for_relfilenode_on_master_and_segments 'test_linking' 2
}