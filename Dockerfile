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

ADD . /credis

RUN cd /credis \
    && git submodule deinit -f . \
    && git submodule update --init 

RUN cd /credis/grpc \
    && git submodule update --init \
    && make -j4 \
    && make -j install

RUN  cd /credis/grpc/third_party/protobuf \
    && make -j install \
    && rm -rf /credis/grpc

RUN cd /credis/protos \
    && make

RUN cd /credis/leveldb \
    && CXXFLAGS=-fPIC make -j

RUN cd /credis/glog \
    && cmake . \
    && make -j install \
    && rm -rf /credis/glog

RUN cd /credis/gflags \
    && mkdir build_; cd build_; cmake .. \
    && make -j \
    && make -j install \
    && rm -rf /credis/gflags

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
