FROM frolvlad/alpine-glibc

# Stuff needed to build native binaries
# RUN apk add build-base gcc abuild binutils binutils-doc gcc-doc

# Stuff needed to use cmake
# RUN apk add cmake cmake-doc extra-cmake-modules extra-cmake-modules-doc

# Stuff needed specifically to set up credis
RUN apk add --update \
      git \
      bash \
      curl \
      linux-headers \
      autoconf \
      automake \
      libtool \
      python \
      python-dev \
      py-pip \
      build-base \
      && rm -rf /var/cache/apk/*

ADD . /credis
# RUN git clone -b etcd.coordinator https://github.com/graysonchao/credis

# Run setup scripts individually to improve Docker build caching.
# Ordering does matter.
RUN bash /credis/thirdparty/scripts/build_glog.sh
RUN bash /credis/thirdparty/scripts/build_leveldb.sh
RUN bash /credis/thirdparty/scripts/build_grpc.sh
RUN bash /credis/thirdparty/scripts/build_protobuf.sh
RUN bash /credis/thirdparty/scripts/build_etcd_grpc.sh
RUN bash /credis/thirdparty/scripts/build_redis.sh
RUN bash /credis/thirdparty/scripts/setup.sh
RUN pushd credis; pushd build; cmake ..; make -j

EXPOSE 6379

ENTRYPOINT credis/redis-server --port 6379 --loadmodule credis/build/src/libmember.so 127.0.0.1 6379
