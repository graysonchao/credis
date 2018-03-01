include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/redis/src/)
include_directories(${CMAKE_SOURCE_DIR}/redis/deps/)

find_package(glog 0.3.5 REQUIRED)
find_package(LevelDB REQUIRED)
find_package(Tcmalloc)
find_package(Protobuf)
find_package(GRPC)

include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${GRPC_INCLUDE_DIR})
include_directories(${LevelDB_INCLUDES})

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/test)

# For redis, build with either of
#   make MALLOC=tcmalloc
# or
#   make MALLOC=jemalloc

# GRPC and Protocol Buffers libraries location
list(APPEND CMAKE_PREFIX_PATH ${GRPC_INCLUDE_DIR} ${Protobuf_INCLUDE_DIRS})

if (Tcmalloc_FOUND)
  set(MALLOC_LIBRARY "${Tcmalloc_LIBRARY}")
else()
  set(MALLOC_LIBRARY "${CMAKE_SOURCE_DIR}/redis/deps/jemalloc/lib/libjemalloc.a")
endif()

set(REDIS_MODULE_CFLAGS -W -Wall -fno-common -g -ggdb -std=c++11 -O2)
if(APPLE)
  set(CMAKE_SHARED_LIBRARY_SUFFIX ".so")
  set(REDIS_MODULE_LDFLAGS "-undefined dynamic_lookup")
else()
  set(REDIS_MODULE_LDFLAGS -shared)
endif()

# Protobufs types.
# file(GLOB PB_SOURCES "${CMAKE_SOURCE_DIR}/protos/src/*" rpc.*)
set(PROTO_SRC_DIR "${CMAKE_SOURCE_DIR}/protos/src")
set(PB_SOURCES
    "${PROTO_SRC_DIR}/auth.grpc.pb.cc"
    "${PROTO_SRC_DIR}/auth.pb.cc"
    "${PROTO_SRC_DIR}/kv.grpc.pb.cc"
    "${PROTO_SRC_DIR}/kv.pb.cc"
    "${PROTO_SRC_DIR}/rpc.grpc.pb.cc"
    "${PROTO_SRC_DIR}/rpc.pb.cc"
    "${PROTO_SRC_DIR}/v3lock.grpc.pb.cc"
    "${PROTO_SRC_DIR}/v3lock.pb.cc"

)
set_source_files_properties(
  ${PB_SOURCES}
    PROPERTIES
    COMPILE_FLAGS "-Wno-unused-variable -Wno-unused-parameter"
)
add_library(pb_types SHARED ${PB_SOURCES})
target_compile_options(pb_types PUBLIC -Wno-unused-variable -Wno-unused-parameter)
target_link_libraries(pb_types gRPC::grpc)
target_link_libraries(pb_types gRPC::grpc++)
target_link_libraries(pb_types ${Protobuf_LIBRARIES})
# For files

# timer
add_library(timer STATIC timer.cc)
target_compile_options(timer PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(timer glog::glog)

# utils
add_library(utils STATIC utils.cc)
target_compile_options(utils PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)

add_library(etcd STATIC etcd/etcd.cc)
target_compile_options(etcd PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(etcd glog::glog)
target_link_libraries(etcd pb_types)

add_library(chain STATIC chain.cc)
target_compile_options(chain PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(chain glog::glog)

# master_client
add_library(master_client STATIC master_client.cc)
target_compile_options(master_client PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(master_client ${REDIS_MODULE_LDFLAGS})
target_link_libraries(master_client ${LevelDB_LIBRARIES})
target_link_libraries(master_client glog::glog)
target_link_libraries(master_client etcd)
target_link_libraries(master_client chain)
# CPPFLAGS += `pkg-config --cflags protobuf grpc`

# client
add_library(client STATIC client.cc)
target_compile_options(client PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(client ${LevelDB_LIBRARIES})
target_link_libraries(client glog::glog)

# master
add_library(master MODULE master.cc)
target_compile_options(master PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(master ${REDIS_MODULE_LDFLAGS})
target_link_libraries(master utils)
target_link_libraries(master glog::glog)
target_link_libraries(master ${LevelDB_LIBRARIES})

# member
add_library(member MODULE member.cc chain.cc)
target_compile_options(member PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(member ${REDIS_MODULE_LDFLAGS})
target_link_libraries(member utils)
target_link_libraries(member glog::glog)
target_link_libraries(member ${LevelDB_LIBRARIES})
target_link_libraries(member master_client)
target_link_libraries(member ${MALLOC_LIBRARY})

# new master (TODO rename from coordinator later)
add_library(coordinator STATIC coordinator.cc)
target_compile_options(coordinator PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(coordinator ${REDIS_MODULE_LDFLAGS})
target_link_libraries(coordinator glog::glog)
target_link_libraries(coordinator etcd)
target_link_libraries(coordinator utils)
target_link_libraries(coordinator chain)

add_executable(run_coordinator run_coordinator.cc)
target_link_libraries(run_coordinator etcd)
target_link_libraries(run_coordinator coordinator)

# Benchmarks.
add_executable(credis_parput_bench credis_parput_bench.cc)
target_link_libraries(credis_parput_bench glog::glog client
  ${CMAKE_SOURCE_DIR}/redis/deps/hiredis/libhiredis.a
  ${CMAKE_SOURCE_DIR}/redis/src/ae.o
  ${CMAKE_SOURCE_DIR}/redis/src/zmalloc.o
  ${MALLOC_LIBRARY} timer)

add_executable(credis_seqput_bench credis_seqput_bench.cc)
target_link_libraries(credis_seqput_bench glog::glog client
  ${CMAKE_SOURCE_DIR}/redis/deps/hiredis/libhiredis.a
  ${CMAKE_SOURCE_DIR}/redis/src/ae.o
  ${CMAKE_SOURCE_DIR}/redis/src/zmalloc.o
  ${MALLOC_LIBRARY} timer)

add_executable(redis_parput_bench redis_parput_bench.cc)
target_link_libraries(redis_parput_bench glog::glog client
  ${CMAKE_SOURCE_DIR}/redis/deps/hiredis/libhiredis.a
  ${CMAKE_SOURCE_DIR}/redis/src/ae.o
  ${CMAKE_SOURCE_DIR}/redis/src/zmalloc.o
  ${MALLOC_LIBRARY} timer)

add_executable(redis_seqput_bench redis_seqput_bench.cc)
target_link_libraries(redis_seqput_bench glog::glog client
  ${CMAKE_SOURCE_DIR}/redis/deps/hiredis/libhiredis.a
  ${CMAKE_SOURCE_DIR}/redis/src/ae.o
  ${CMAKE_SOURCE_DIR}/redis/src/zmalloc.o
  ${MALLOC_LIBRARY} timer)
