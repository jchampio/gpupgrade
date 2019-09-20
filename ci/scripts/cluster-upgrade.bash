#!/bin/bash

set -eux -o pipefail

# We'll need this to transfer our built binaries over to the cluster hosts.
./ccp_src/scripts/setup_ssh_to_cluster.sh

# Cache our list of hosts to loop over below.
mapfile -t hosts < cluster_env_files/hostfile_all

# Copy over the SQL dump we pulled from master.
scp sqldump/dump.sql.xz gpadmin@mdw:/tmp/

# Build gpupgrade.
export GOPATH=$PWD/go
export PATH=$GOPATH/bin:$PATH

cd $GOPATH/src/github.com/greenplum-db/gpupgrade
make depend
make

# Install gpupgrade onto the cluster machines.
for host in "${hosts[@]}"; do
    scp gpupgrade "gpadmin@$host:/usr/local/greenplum-db-devel/bin/"
done

# Load the SQL dump into the cluster.
echo 'Loading SQL dump...'
time ssh mdw bash <<"EOF"
    set -eux -o pipefail

    source /usr/local/greenplum-db-devel/greenplum_path.sh
    export PGOPTIONS='--client-min-messages=warning'
    unxz < /tmp/dump.sql.xz | psql -f - postgres
EOF

# Now do the upgrade.
time ssh mdw bash <<"EOF"
    set -eu -o pipefail

    source /usr/local/greenplum-db-devel/greenplum_path.sh
    export PGPORT=5432 # TODO remove the need for this

    wait_for_step() {
        local step="$1"
        local timeout=${2:-60} # default to 60 seconds
        local done=0

        for i in $(seq $timeout); do
            local output=$(gpupgrade status upgrade)
            if [ "$?" -ne "0" ]; then
                echo "$output"
                exit 1
            fi

            local status=$(grep "$step" <<< "$output")

            if [[ $status = *FAILED* ]]; then
                echo "$output"
                exit 1
            fi

            if [[ $status = *"COMPLETE - $step"* ]]; then
                done=1
                echo "$status"
                break
            fi

            sleep 1
        done

        if (( ! $done )); then
            echo "ERROR: timed out waiting for '${step}' to complete"
            exit 1
        fi
    }

    dump_sql() {
        local port=$1
        local dumpfile=$2

        echo "Dumping cluster contents from port ${port} to ${dumpfile}..."

        ssh -n mdw "
            source /usr/local/greenplum-db-devel/greenplum_path.sh
            pg_dumpall -p ${port} -f '$dumpfile'
        "
    }

    compare_dumps() {
        local old_dump=$1
        local new_dump=$2

        echo "Comparing dumps at ${old_dump} and ${new_dump}..."

        ssh -n mdw "
            diff -U3 --speed-large-files --ignore-space-change '$old_dump' '$new_dump'
        "
    }

    dump_sql 5432 /tmp/old.sql

    gpupgrade prepare init \
              --new-bindir /usr/local/greenplum-db-devel/bin \
              --old-bindir /usr/local/greenplum-db-devel/bin

    gpupgrade prepare start-hub

    gpupgrade check config
    gpupgrade check version
    gpupgrade check seginstall

    gpupgrade prepare start-agents

    gpupgrade prepare init-cluster
    wait_for_step "Initialize new cluster"

    gpupgrade prepare shutdown-clusters
    wait_for_step "Shutdown clusters"

    gpupgrade upgrade convert-master

    gpupgrade upgrade copy-master
    wait_for_step "Copy master data directory to segments"

    gpupgrade upgrade convert-primaries
    wait_for_step "Run pg_upgrade on primaries" 1200 # twenty minute timeout

    gpupgrade upgrade validate-start-cluster
    wait_for_step "Validate the upgraded cluster can start up"

    dump_sql 5433 /tmp/new.sql
    if ! compare_dumps /tmp/old.sql /tmp/new.sql; then
        echo 'error: before and after dumps differ'
        exit 1
    fi

    echo 'Upgrade successful.'
EOF
