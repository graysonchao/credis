#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e



TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../

glog_vname="0.3.5"
if [ ! -d $TP_DIR/pkg/glog ]; then
  # This check is to make sure the tarball has been fully extracted. The only
  # relevant bit about glog/utils/whatisdoing.sh is that it is one of the last
  # files in the tarball.
  ######if [ ! -f $TP_DIR/pkg/glog/utils/whatisdoing.sh ]; then
    mkdir -p "$TP_DIR/pkg/glog"
    curl -sL "https://github.com/google/glog/archive/v$glog_vname.tar.gz" | tar xz --strip-components=1 -C "$TP_DIR/pkg/glog"
  #######fi
  pushd $TP_DIR/pkg/glog
  ./configure && make -j && make -j install
  popd
fi
