#!/bin/bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
TP_DIR=$TP_SCRIPT_DIR/..

mkdir -p $TP_DIR/build
mkdir -p $TP_DIR/pkg

if [[ -z  "$1" ]]; then
  PYTHON_EXECUTABLE=`which python`
else
  PYTHON_EXECUTABLE=$1
fi
echo "Using Python executable $PYTHON_EXECUTABLE."

unamestr="$(uname)"

##############################################
# redis
##############################################
bash "$TP_SCRIPT_DIR/build_redis.sh"

##############################################
# glog
##############################################
bash "$TP_SCRIPT_DIR/build_glog.sh"

##############################################
# grpc
##############################################
bash "$TP_SCRIPT_DIR/build_grpc.sh"

##############################################
# leveldb
##############################################
bash "$TP_SCRIPT_DIR/build_leveldb.sh"

##############################################
# protobuf
##############################################
bash "$TP_SCRIPT_DIR/build_protobuf.sh"

##############################################
# etcd grpc bindings
##############################################
bash "$TP_SCRIPT_DIR/build_etcd_grpc.sh"
