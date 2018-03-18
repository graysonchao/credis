find_package(Protobuf REQUIRED)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/etcd3/cmake/Modules)
find_package(GRPC REQUIRED)
# TODO: actually install etcd3 and find it.
include_directories(${CMAKE_SOURCE_DIR}/etcd3)
add_library(etcd3
        ${CMAKE_SOURCE_DIR}/etcd3/src/etcd3.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/auth.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/kv.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/rpc.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/v3lock.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/auth.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/kv.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/rpc.grpc.pb.cc
        ${CMAKE_SOURCE_DIR}/etcd3/proto-src/v3lock.grpc.pb.cc
)
target_link_libraries(etcd3 gRPC::grpc++)

add_library(etcd_utils SHARED etcd_utils.cc)

# etcd agent
# add_library(etcd_agent MODULE agent/etcd_agent.cc agent/etcd_agent_module.cc)
# target_compile_options(etcd_agent PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
# target_link_libraries(etcd_agent ${REDIS_MODULE_LDFLAGS})
# target_link_libraries(etcd_agent ${LevelDB_LIBRARIES})
# target_link_libraries(etcd_agent etcd3)
# target_link_libraries(etcd_agent glog::glog)
# target_link_libraries(etcd_agent etcd_utils)
# target_link_libraries(etcd_agent utils)

# etcd master
add_library(etcd_master SHARED
        master/etcd_master_module.cc
        master/heartbeat_monitor.cc)
target_compile_options(etcd_master PUBLIC ${REDIS_MODULE_CFLAGS} -fPIC)
target_link_libraries(etcd_master ${REDIS_MODULE_LDFLAGS})
target_link_libraries(etcd_master ${PROTOBUF_LIBRARIES})
target_link_libraries(etcd_master ${LevelDB_LIBRARIES})
target_link_libraries(etcd_master glog::glog)
target_link_libraries(etcd_master etcd3)
target_link_libraries(etcd_master etcd_utils)
target_link_libraries(etcd_master utils)
