# CMakeLists.txt
cmake_minimum_required(VERSION 3.16)
project(RaftKVStore LANGUAGES CXX)

# C++标准
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 编译选项
add_compile_options(-Wall -Wextra -Wpedantic)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-g -O0)
else()
    add_compile_options(-O3)
endif()

# 查找依赖包
find_package(gRPC CONFIG REQUIRED)
find_package(GTest REQUIRED)

# RocksDB
find_path(ROCKSDB_INCLUDE_DIR "rocksdb/db.h")
find_library(ROCKSDB_LIB rocksdb)

if(NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIB)
    message(FATAL_ERROR "RocksDB not found")
endif()

# 设置include目录
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_BINARY_DIR}/src/proto
    ${PROTOBUF_INCLUDE_DIRS}
    ${ROCKSDB_INCLUDE_DIR}
)

# 添加子目录
add_subdirectory(src)
add_subdirectory(tests)
add_subdirectory(example)

# 打印配置信息
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ Standard: ${CMAKE_CXX_STANDARD}")

# 在配置阶段执行命令，将config.json文件复制到build/test文件夹下
execute_process(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/config.json ${CMAKE_BINARY_DIR}/tests/config.json)
execute_process(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/config.json ${CMAKE_BINARY_DIR}/config.json)
execute_process(COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_SOURCE_DIR}/config.json ${CMAKE_BINARY_DIR}/example/config.json)
