#!/usr/bin/env bash

set -x

# Cause the script to exit if a single command fails.
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)/../
PKG_DIR="$TP_DIR/pkg/etcd_grpc"

if [[ ! -f $PKG_DIR/src/rpc.pb.h ]]; then
    etcd_vname="3.3.1"

    if [[ ! -d "$PKG_DIR/etcd" ]] ; then
        mkdir -p "$PKG_DIR/etcd"
        curl -sL "https://github.com/coreos/etcd/releases/download/v$etcd_vname/v$etcd_vname.tar.gz" | \
            tar xz --strip-components=1 -C "$PKG_DIR/etcd"
    fi

    # Copy protobufs files and eliminate unnecessary dependencies, such as Go-only features
    mkdir -p "$PKG_DIR/original"
    mkdir -p "$PKG_DIR/protos"
    for f in {rpc.proto,kv.proto,auth.proto,v3lock.proto,v3election.proto}; do
        file=$(find "$PKG_DIR/etcd" -name "*$f")
        /usr/bin/env python $(dirname $0)/strip_dependencies.py $file > $PKG_DIR/protos/$f
    done

    mkdir -p "$PKG_DIR/etcd_grpc"
    protoc -I$PKG_DIR/protos \
        --grpc_out=generate_mock_code=true:$PKG_DIR/etcd_grpc\
        --cpp_out=$PKG_DIR/etcd_grpc\
        --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin)\
        $PKG_DIR/protos/*
fi
