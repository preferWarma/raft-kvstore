# 要求 CMake ≥ 3.27
cmake_minimum_required(VERSION 3.27)

# 查找依赖
find_package(Protobuf CONFIG REQUIRED) # protobuf::protoc
find_package(gRPC CONFIG REQUIRED) # gRPC::grpc_cpp_plugin

# 收集 .proto 文件
file(GLOB PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")

# 创建库（稍后再填源文件）
add_library(proto_lib STATIC)

# 生成 protobuf 消息代码 (*.pb.cc / *.pb.h)
protobuf_generate(
    TARGET proto_lib
    PROTOS ${PROTO_FILES}
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
)

# 生成 gRPC 服务代码 (*.grpc.pb.cc / *.grpc.pb.h)
protobuf_generate(
    TARGET proto_lib
    PROTOS ${PROTO_FILES}
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.cc .grpc.pb.h
    PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
)

# 头文件搜索路径
target_include_directories(
    proto_lib
    PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>
)

# 链接依赖库
target_link_libraries(
    proto_lib
    PUBLIC
    protobuf::libprotobuf
    gRPC::grpc++
)

# 供上层目录使用
set(PROTO_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR} PARENT_SCOPE)