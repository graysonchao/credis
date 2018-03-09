cmake_minimum_required(VERSION 3.4)
add_compile_options(-std=c++11)

project(credis)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})

include(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/scripts/thirdparty.cmake)
include_directories(thirdparty/pkg/etcd_grpc/etcd_grpc)

enable_testing()

add_subdirectory(src)
