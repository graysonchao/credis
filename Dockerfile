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

ADD . /

RUN cd /credis \
    && git submodule deinit --force --all \
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
    && mkdir /credis/build; cd /credis/build ; cmake .. ; make -j

# symlinks and handy scripts
RUN ln -s /credis/build/src/run_coordinator \
    && ln -s /credis/redis/src/redis-server \
    && ln -s /credis/redis/src/redis-cli \
    && ln -s /credis/build/src/libmember.so \
    && ln -s /credis/run-member.sh
