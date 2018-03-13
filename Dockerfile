FROM ubuntu:xenial

# general build deps
RUN apt-get update \
    && apt-get install -y \
    git \
    cmake \
    build-essential \
    autoconf \
    libtool \
    libgflags-dev \
    libgoogle-perftools-dev \
    libgtest-dev \
    pkg-config

RUN git clone -b etcd.coordinator https://github.com/graysonchao/credis /credis \
    && cd /credis \
    && git submodule update --init \
    && cd /credis/grpc \
    && git submodule update --init \
    && make -j4 \
    && make -j install \
    && cd /credis/grpc/third_party/protobuf \
    && make -j install \
    && rm -rf /credis/grpc \
    && cd /credis/protos \
    && make \
    && cd /credis/leveldb \
    && CXXFLAGS=-fPIC make -j \
    && cd /credis/glog \
    && cmake . \
    && make -j install \
    && rm -rf /credis/glog

# build redis
RUN cd /credis/redis \
    && env USE_TCMALLOC=yes make -j \
    && mkdir /credis/build; cd /credis/build ; cmake .. ; make -j  \
    && cd / \
    && ln -s /credis/build/src/run_coordinator \
    && ln -s /credis/redis/src/redis-server \
    && ln -s /credis/redis/src/redis-cli
