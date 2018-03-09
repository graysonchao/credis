#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e



TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../

leveldb_vname="v1.20"
if [ ! -d $TP_DIR/pkg/leveldb ]; then
  git clone -b$leveldb_vname https://github.com/google/leveldb $TP_DIR/pkg/leveldb
fi
pushd $TP_DIR/pkg/leveldb
  make -j
popd
