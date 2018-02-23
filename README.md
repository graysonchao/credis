# Chain Replicated Redis

## Building

```
git submodule init
git submodule update

# Install tcmalloc according to
# https://github.com/gperftools/gperftools/blob/master/INSTALL

pushd redis && env USE_TCMALLOC=yes make -j && popd
pushd glog && cmake . && make -j install && popd
pushd leveldb && make -j && popd

# Generate protobufs source files for etcd.
# You must have grpc, protobufs, and protobuf-c installed.
# https://github.com/grpc/grpc/blob/master/INSTALL.md
# https://github.com/google/protobuf/blob/master/src/README.md
# https://github.com/protobuf-c/protobuf-c/blob/master/README.md
# protobuf-c is to support building pure-C Protobufs symbols for Redis modules.
pushd protos && make && popd

mkdir build; cd build
cmake ..
make -j
```

## Setting up etcd
Use the Procfile script. It starts up 3 nodes and a JSON GRPC proxy, which the
master uses to communicate with etcd.
```
go get github.com/mattn/goreman
goreman start
```

## Trying it out

First we start the master and two chain members:

```
cd build/src
# Start the master
../../redis/src/redis-server --loadmodule libmaster.so --port 6369
# Start the first chain members
../../redis/src/redis-server --loadmodule libmember.so --port 6370
../../redis/src/redis-server --loadmodule libmember.so --port 6371
```

Now we register the chain members with the master:

```
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6370
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6371
```

Do some write requests to the first server:

```
redis-cli -p 6370
> MEMBER.PUT a 1
> MEMBER.PUT b 2
```

Add a new tail:

```
../../redis/src/redis-server --loadmodule libmember.so --port 6372
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6372
```

Check that replication worked:

```
redis-cli -p 6372
> get a
```
