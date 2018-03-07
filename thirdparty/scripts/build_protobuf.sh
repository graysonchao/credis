#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../

if [ ! -d $TP_DIR/pkg/protobuf ]; then
  protobuf_vname="3.5.1"
  mkdir -p "$TP_DIR/pkg/protobuf"
  pbv=$protobuf_vname
  curl -sL "https://github.com/google/protobuf/releases/download/v$pbv/protobuf-cpp-$pbv.tar.gz" | \
    tar xz --strip-components=1 -C "$TP_DIR/pkg/protobuf"
  pushd $TP_DIR/pkg/protobuf
    ./configure
    # You can adjust num_jobs if you are fancy and have more cores.
    make -j4
    make -j4 check
    make -j4 install
    if which ldconfig ; then ldconfig ; fi
  popd
fi
