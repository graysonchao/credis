cmake_minimum_required(VERSION 3.0.2)

# Download and unpack googletest at configure time
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/DownloadGTest.cmake
  ${CMAKE_BINARY_DIR}/googletest-download/CMakeLists.txt
)
execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "CMake step for googletest failed: ${result}")
endif()
execute_process(COMMAND ${CMAKE_COMMAND} --build .
  RESULT_VARIABLE result
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/googletest-download )
if(result)
  message(FATAL_ERROR "Build step for googletest failed: ${result}")
endif()

# Prevent overriding the parent project's compiler/linker
# settings on Windows
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)

# Add googletest directly to our build. This defines
# the gtest and gtest_main targets.
add_subdirectory(${CMAKE_BINARY_DIR}/googletest-src
                 ${CMAKE_BINARY_DIR}/googletest-build
                 EXCLUDE_FROM_ALL)

# The gtest/gtest_main targets carry header search path
# dependencies automatically when using CMake 2.8.11 or
# later. Otherwise we have to add them here ourselves.
if (CMAKE_VERSION VERSION_LESS 2.8.11)
  include_directories("${gtest_SOURCE_DIR}/include")
endif()

# Unit tests.
# Linking with gtest_main lets GTest implement the main() fn (test runner)
add_executable(test_chain test_chain.cc)
target_include_directories(test_chain PUBLIC "${CMAKE_SOURCE_DIR}/src")
target_link_libraries(test_chain gtest_main)
target_link_libraries(test_chain gmock)
target_link_libraries(test_chain chain)
add_test(NAME test_chain COMMAND test_chain)
