# 收集所有源文件
file(GLOB_RECURSE RAFT_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp"
)

# 创建raft库
add_library(raft_lib STATIC ${RAFT_SOURCES})

target_link_libraries(raft_lib
    rpc_lib
    proto_lib
    storage_lib
)

# 设置包含目录
target_include_directories(raft_lib PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${PROTO_INCLUDE_DIR}
)