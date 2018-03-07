# Chain Replicated Redis

## Building

```
git submodule init
git submodule update

# Install tcmalloc according to
# https://github.com/gperftools/gperftools/blob/master/INSTALL

# You must have protobuf-cpp installed. If you don't, see:
# https://github.com/google/protobuf/blob/master/src/README.md

pushd third_party;
pushd redis && env USE_TCMALLOC=yes make -j && popd
pushd glog && cmake . && make -j install && popd
pushd leveldb && make -j && popd
pushd grpc && git submodule update --init && make -j && make -j install && popd
popd third_party;

# Generate protobufs source files for etcd.
pushd protos && make && popd

mkdir build; cd build
cmake ..
make -j
```

## Setting up etcd
The simplest way to set up etcd for local testing is with Docker. Any stock image will do.
The conventional etcd client request port is 2379.
```
docker run -d -p 2379:2379 -p 2380:2380 appcelerator/etcd
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

## Running unit tests
From the build directory,
```
$ pwd
/Users/gchao/code/credis/build
$ make test # run tests
```
