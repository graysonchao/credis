find_package(Protobuf REQUIRED)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/etcd3/cmake/Modules)
find_package(GRPC REQUIRED)

set(PB_SOURCES
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/auth.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/kv.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/rpc.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/v3lock.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/auth.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/kv.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/rpc.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/v3lock.grpc.pb.cc
)

set(PB_HEADERS
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/auth.pb.h
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/kv.pb.h
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/rpc.pb.h
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/v3lock.pb.h
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/auth.grpc.pb.h
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/kv.grpc.pb.h
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/rpc.grpc.pb.h
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/v3lock.grpc.pb.h
)

# etcd client
include_directories(${CMAKE_SOURCE_DIR}/etcd3)
add_library(etcd3
        ${CMAKE_SOURCE_DIR}/etcd3/src/etcd3.cc
        ${PB_SOURCES}
)

target_link_libraries(etcd3 gRPC::grpc++)
set_source_files_properties(
        ${PB_SOURCES} ${PB_HEADERS}
        PROPERTIES
        COMPILE_FLAGS "-Wno-unused-variable -Wno-unused-parameter"
)

add_library(etcd_utils SHARED etcd_utils.cc)
