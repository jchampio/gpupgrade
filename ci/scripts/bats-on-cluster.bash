#!/bin/bash
#
# Copyright (c) 2017-2020 VMware, Inc. or its affiliates
# SPDX-License-Identifier: Apache-2.0

set -eux -o pipefail

drop_gphdfs_roles() {
  echo 'Dropping gphdfs role...'
  ssh mdw "
      set -x

      source /usr/local/greenplum-db-source/greenplum_path.sh
      export MASTER_DATA_DIRECTORY=/data/gpdata/master/gpseg-1

      psql -d postgres <<SQL_EOF
          CREATE OR REPLACE FUNCTION drop_gphdfs() RETURNS VOID AS \\\$\\\$
          DECLARE
            rolerow RECORD;
          BEGIN
            RAISE NOTICE 'Dropping gphdfs users...';
            FOR rolerow IN SELECT * FROM pg_catalog.pg_roles LOOP
              EXECUTE 'alter role '
                || quote_ident(rolerow.rolname) || ' '
                || 'NOCREATEEXTTABLE(protocol=''gphdfs'',type=''readable'')';
              EXECUTE 'alter role '
                || quote_ident(rolerow.rolname) || ' '
                || 'NOCREATEEXTTABLE(protocol=''gphdfs'',type=''writable'')';
              RAISE NOTICE 'dropping gphdfs from role % ...', quote_ident(rolerow.rolname);
            END LOOP;
          END;
          \\\$\\\$ LANGUAGE plpgsql;

          SELECT drop_gphdfs();

          DROP FUNCTION drop_gphdfs();
SQL_EOF
  "
}

#
# MAIN
#

# TODO: combine this or at least pull out common functions with upgrade-cluster.bash?

# This port is selected by our CI pipeline
MASTER_PORT=5432

# We'll need this to transfer our built binaries over to the cluster hosts.
./ccp_src/scripts/setup_ssh_to_cluster.sh

# Cache our list of hosts to loop over below.
mapfile -t hosts < cluster_env_files/hostfile_all

export GPHOME_SOURCE=/usr/local/greenplum-db-source
export GPHOME_TARGET=/usr/local/greenplum-db-target

# Install gpupgrade binary onto the cluster machines.
chmod +x bin_gpupgrade/gpupgrade
for host in "${hosts[@]}"; do
    scp bin_gpupgrade/gpupgrade "gpadmin@$host:/tmp"
    ssh centos@$host "sudo mv /tmp/gpupgrade /usr/local/bin"
done

drop_gphdfs_roles

time ssh mdw bash <<EOF
    set -eux -o pipefail

    echo "HELLO WORLD"
EOF

echo 'bats test successful.'
