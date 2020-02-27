#!/bin/bash

set -eux -o pipefail

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

echo 'Loading SQL dump into old cluster...'
time ssh mdw bash <<EOF
    set -eux -o pipefail

    source ${GPHOME_OLD}/greenplum_path.sh
    export PGOPTIONS='--client-min-messages=warning'
    unxz < /tmp/dump.sql.xz | psql -f - postgres

    psql -f - postgres <<ENDSQL
CREATE TABLE customer_addresses_dim (
    customer_address_id integer NOT NULL,
    customer_id integer NOT NULL,
    valid_from_timestamp timestamp without time zone DEFAULT now() NOT NULL,
    valid_to_timestamp timestamp without time zone,
    house_number character varying(20),
    street_name character varying(150),
    appt_suite_no character varying(50),
    city character varying(200),
    state_code character varying(2),
    zip_code character varying(5),
    zip_plus_four character varying(10),
    country character varying(10),
    phone_number character varying(20)
)
WITH (appendonly=true, compresstype=quicklz, orientation=row) DISTRIBUTED BY (customer_id);
ALTER TABLE ONLY customer_addresses_dim ALTER COLUMN customer_id SET STATISTICS 1000;
ALTER TABLE ONLY customer_addresses_dim ALTER COLUMN valid_to_timestamp SET STATISTICS 1000;
ENDSQL
EOF

# Dump the old cluster for later comparison.
dump_sql 5432 /tmp/old.sql

# Now do the upgrade.
time ssh mdw bash <<EOF
    set -eux -o pipefail

    gpupgrade initialize \
              --new-bindir ${GPHOME_NEW}/bin \
              --old-bindir ${GPHOME_OLD}/bin \
              --old-port 5432

    gpupgrade execute
    gpupgrade finalize
EOF

# Dump the new cluster and compare.
dump_sql 5432 /tmp/new.sql
if ! compare_dumps /tmp/old.sql /tmp/new.sql; then
    echo 'error: before and after dumps differ'
    exit 1
fi

echo 'Upgrade successful.'
