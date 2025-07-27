# 收集所有源文件
file(GLOB_RECURSE RPC_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

# 创建rpc库
add_library(rpc_lib STATIC ${RPC_SOURCES})

target_link_libraries(rpc_lib
    storage_lib
    proto_lib
    gRPC::grpc++
    Threads::Threads
)

# 设置包含目录
target_include_directories(rpc_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${PROTO_INCLUDE_DIR}
)