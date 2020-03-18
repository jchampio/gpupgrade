#!/bin/bash

set -eux -o pipefail
dirpath=$(dirname "${0}")
source "${dirpath}/../../test/finalize_checks.bash"

dump_sql() {
    local port=$1
    local dumpfile=$2

    echo "Dumping cluster contents from port ${port} to ${dumpfile}..."

    ssh -n mdw "
        source ${GPHOME_NEW}/greenplum_path.sh
        pg_dumpall -p ${port} -f '$dumpfile'
    "
}

compare_dumps() {
    local old_dump=$1
    local new_dump=$2

    echo "Comparing dumps at ${old_dump} and ${new_dump}..."

    # 5 to 6 requires some massaging of the diff due to expected changes.
    if (( $FILTER_DIFF )); then
        go build ./ci/scripts/filter
        scp ./filter mdw:/tmp/filter

        # First filter out any algorithmically-fixable differences, then
        # patch out the remaining expected diffs explicitly.
        ssh mdw "
            /tmp/filter < '$new_dump' > '$new_dump.filtered'
            patch -R '$new_dump.filtered'
        " < ./ci/scripts/filter/acceptable_diff

        new_dump="$new_dump.filtered"
    fi

    ssh -n mdw "
        diff -U3 --speed-large-files --ignore-space-change --ignore-blank-lines '$old_dump' '$new_dump'
    "
}

# Retrieves the installed GPHOME for a given GPDB RPM.
rpm_gphome() {
    local package_name=$1

    local version=$(ssh -n gpadmin@mdw rpm -q --qf '%{version}' "$package_name")
    echo /usr/local/greenplum-db-$version
}

#
# MAIN
#

# This port is selected by our CI pipeline
MASTER_PORT=5432

# We'll need this to transfer our built binaries over to the cluster hosts.
./ccp_src/scripts/setup_ssh_to_cluster.sh

# Cache our list of hosts to loop over below.
mapfile -t hosts < cluster_env_files/hostfile_all

# Copy over the SQL dump we pulled from master.
scp sqldump/dump.sql.xz gpadmin@mdw:/tmp/

# Figure out where GPHOMEs are.
export GPHOME_OLD=$(rpm_gphome ${OLD_PACKAGE})
export GPHOME_NEW=$(rpm_gphome ${NEW_PACKAGE})

# Build gpupgrade.
export GOPATH=$PWD/go
export PATH=$GOPATH/bin:$PATH

cd $GOPATH/src/github.com/greenplum-db/gpupgrade
make depend
make

# Install gpupgrade binary onto the cluster machines.
for host in "${hosts[@]}"; do
    scp gpupgrade "gpadmin@$host:/tmp"
    ssh centos@$host "sudo mv /tmp/gpupgrade /usr/local/bin"
done

echo 'Loading SQL dump into source cluster...'
time ssh mdw bash <<EOF
    set -eux -o pipefail

    source ${GPHOME_OLD}/greenplum_path.sh
    export PGOPTIONS='--client-min-messages=warning'
    unxz < /tmp/dump.sql.xz | psql -f - postgres
EOF

# Dump the old cluster for later comparison.
dump_sql $MASTER_PORT /tmp/old.sql

# Install TPC-H.
time ssh mdw 'cat >> ~/.bashrc' <<EOF
source /usr/local/greenplum-db-5*/greenplum_path.sh
export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1
EOF
time ssh mdw createdb gpadmin

time ssh centos@mdw bash <<< '
    sudo su root
    set -eux -o pipefail

    cd
    curl https://raw.githubusercontent.com/pivotalguru/TPC-H/master/tpch.sh > tpch.sh
    chmod 755 tpch.sh

    cat > tpch_variables.sh <<EOF
REPO="TPC-H"
REPO_URL="https://github.com/pivotalguru/TPC-H"
ADMIN_USER="gpadmin"
INSTALL_DIR="/pivotalguru"
EXPLAIN_ANALYZE="false"
RANDOM_DISTRIBUTION="false"
MULTI_USER_COUNT="5"
GEN_DATA_SCALE="1"
SINGLE_USER_ITERATIONS="1"
RUN_COMPILE_TPCH="false"
RUN_GEN_DATA="false"
RUN_INIT="true"
RUN_DDL="true"
RUN_LOAD="true"
RUN_SQL="false"
RUN_SINGLE_USER_REPORT="false"
RUN_MULTI_USER="false"
RUN_MULTI_USER_REPORT="false"
EOF

    # Preinstall the TPC-H utility so we can modify it a bit.
    yum -y install git || true
    mkdir -p /pivotalguru/TPC-H
    chown gpadmin /pivotalguru/TPC-H
    su -c "cd /pivotalguru/TPC-H; git clone --depth=1 //github.com/pivotalguru/TPC-H" gpadmin

    # Do not run the reports.
    rm -rf /pivotalguru/TPC-H/0{5..8}*

    ./tpch.sh
'

echo "failing early"
exit 1


time ssh mdw bash <<EOF
    set -eux -o pipefail

    gpupgrade initialize \
              --target-bindir ${GPHOME_NEW}/bin \
              --source-bindir ${GPHOME_OLD}/bin \
              --source-master-port $MASTER_PORT

    gpupgrade execute
    gpupgrade finalize
EOF

# TODO: how do we know the cluster upgraded?  5 to 6 is a version check; 6 to 6 ?????
#   currently, it's sleight of hand...old is on port $MASTER_PORT then new is!!!!
#   perhaps use the controldata("pg_controldata $MASTER_DATA_DIR") system identifier?

# Dump the new cluster and compare.
dump_sql $MASTER_PORT /tmp/new.sql
if ! compare_dumps /tmp/old.sql /tmp/new.sql; then
    echo 'error: before and after dumps differ'
    exit 1
fi

# Test that mirrors actually work
echo 'Doing failover tests of mirrors...'
check_mirror_validity "${GPHOME_NEW}" mdw $MASTER_PORT

echo 'Upgrade successful.'
