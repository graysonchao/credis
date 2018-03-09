
# add all thirdparty related path definitions here

list(APPEND CMAKE_MODULE_PATH
            ${CMAKE_SOURCE_DIR}/thirdparty/pkg/leveldb)
list(APPEND CMAKE_MODULE_PATH
            ${CMAKE_SOURCE_DIR}/thirdparty/pkg/redis)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/pkg/redis/src)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/pkg/redis/deps)
list(APPEND CMAKE_MODULE_PATH
            ${CMAKE_SOURCE_DIR}/thirdparty/pkg/glog)
list(APPEND CMAKE_MODULE_PATH
            ${CMAKE_SOURCE_DIR}/thirdparty/pkg/grpc)
list(APPEND CMAKE_MODULE_PATH
            ${CMAKE_SOURCE_DIR}/thirdparty/pkg/protobuf)
list(APPEND CMAKE_MODULE_PATH
            ${CMAKE_SOURCE_DIR}/thirdparty/pkg/etcd_grpc)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/pkg/etcd_grpc)
