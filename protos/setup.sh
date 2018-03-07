#!/bin/bash

set -e
set -x

etcd_version="3.3.1"

CWD=$(dirname $0)

mkdir -p "$CWD/etcd"
curl -sL "https://github.com/coreos/etcd/releases/download/v$etcd_version/v$etcd_version.tar.gz" | \
    tar xz --strip-components=1 -C "$CWD/etcd"

mkdir -p "$CWD/original"
for f in {rpc.proto,kv.proto,auth.proto,v3lock.proto,v3election.proto}; do
    find "$CWD/etcd" -name "*$f" -exec cp {} $CWD/original \;
    /usr/bin/env python $CWD/strip_dependencies.py $CWD/original/$f > $CWD/$f
done

rm -rf "$CWD/original"
rm -rf "$CWD/etcd"

