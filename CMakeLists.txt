cmake_minimum_required(VERSION 3.4)
add_compile_options(-std=c++11)

project(credis)

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR})

enable_testing()

add_subdirectory(src)
