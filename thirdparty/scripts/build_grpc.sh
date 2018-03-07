#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e



TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../

grpc_vname="v1.10.0"
if [ ! -d $TP_DIR/pkg/grpc/ ]; then
  # This check is to make sure the tarball has been fully extracted. The only
  # relevant bit about grpc/utils/whatisdoing.sh is that it is one of the last
  # files in the tarball.
  git clone -b$grpc_vname https://github.com/grpc/grpc $TP_DIR/pkg/grpc
  pushd $TP_DIR/pkg/grpc
    git submodule update --init
    make -j
    make -j install
  popd
fi
