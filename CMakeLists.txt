# CMakeLists.txt
cmake_minimum_required(VERSION 3.16)
project(RaftKVStore VERSION 1.0.0 LANGUAGES CXX)

# C++17标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 编译选项
add_compile_options(-Wall -Wextra -Wpedantic)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-g -O0)
else()
    add_compile_options(-O3)
endif()

# 查找依赖包
find_package(Threads REQUIRED)

# 移除重复的Protobuf查找，由gRPC自动引入
# find_package(Protobuf REQUIRED)
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