#!/bin/bash

set -ex

export GOPATH=$PWD/go
export PATH=$GOPATH/bin:$PATH

cd $GOPATH/src/github.com/greenplum-db/gpupgrade
    make depend
    make

    # todo: is there a cleaner way to get this format?
    version=$(git describe | perl -pe 's/(.*)-([0-9]*)-(g[0-9a-f]*)/\1+dev.\2.\3/')
cd -

cp $GOPATH/src/github.com/greenplum-db/gpupgrade/gpupgrade gpupgrade_bin_path/gpupgrade-rc-$version-linux_x86_64
